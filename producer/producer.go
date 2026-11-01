package producer

import (
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/pkg/errors"
)

const (
	maxEncoderReprLength = 4096
)

// ErrShuttingDown is reported to synchronous callers for admissions that
// never reached a terminal broker acknowledgement because the producer was
// stopped.
var ErrShuttingDown = errors.New("producer is shutting down")

// quotaToken is the producer-side view of a byte budget reservation. The
// concrete implementation is budget.Reservation; the producer depends on the
// interface to avoid an import cycle.
type quotaToken interface {
	Release()
}

// produceMeta travels with the sarama.ProducerMessage and keeps the
// response channel of a request associated with the budget ownership of the
// very same message, so the reserved bytes are released only once that
// message reaches its terminal state.
type produceMeta struct {
	replyCh chan Response
	token   quotaToken
}

// T builds on top of `sarama.AsyncProducer` to improve the shutdown handling.
// The problem it solves is that `sarama.AsyncProducer` drops all buffered
// messages as soon as it is ordered to shutdown. On the contrary, when `T` is
// ordered to stop it allows some time for the buffered messages to be
// committed to the Kafka cluster, and only when that time has elapsed it drops
// uncommitted messages.
//
// TODO Consider implementing some sort of dead message processing.
type T struct {
	mergActDesc     *actor.Descriptor
	dispActDesc     *actor.Descriptor
	saramaClient    sarama.Client
	saramaProducer  sarama.AsyncProducer
	shutdownTimeout time.Duration
	dispatcherCh    chan *sarama.ProducerMessage
	responseCh      chan Response
	wg              sync.WaitGroup

	// To be used in tests only
	testDroppedMsgCh chan<- *sarama.ProducerMessage
}

type Response struct {
	Msg *sarama.ProducerMessage
	Err error
}

// Spawn creates a producer instance and starts its internal goroutines.
func Spawn(parentActDesc *actor.Descriptor, cfg *config.Proxy) (*T, error) {
	saramaCfg := cfg.SaramaProducerCfg()
	saramaCfg.Producer.Return.Successes = true
	saramaCfg.Producer.Return.Errors = true

	saramaClient, err := sarama.NewClient(cfg.Kafka.SeedPeers, saramaCfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create sarama.Client")
	}
	saramaProducer, err := sarama.NewAsyncProducerFromClient(saramaClient)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create sarama.Producer")
	}

	p := &T{
		mergActDesc:     parentActDesc.NewChild("prod_merg"),
		dispActDesc:     parentActDesc.NewChild("prod_disp"),
		saramaClient:    saramaClient,
		saramaProducer:  saramaProducer,
		shutdownTimeout: cfg.Producer.ShutdownTimeout,
		dispatcherCh:    make(chan *sarama.ProducerMessage, cfg.Producer.ChannelBufferSize),
		responseCh:      make(chan Response, cfg.Producer.ChannelBufferSize),
	}
	actor.Spawn(p.mergActDesc, &p.wg, p.runMerger)
	actor.Spawn(p.dispActDesc, &p.wg, p.runDispatcher)
	return p, nil
}

// Stop shuts down all producer goroutines and releases all resources.
func (p *T) Stop() {
	close(p.dispatcherCh)
	p.wg.Wait()
}

// Produce submits a message to the specified `topic` of the Kafka cluster
// using `key` to identify a destination partition. The exact algorithm used to
// map keys to partitions is implementation specific but it is guaranteed that
// it returns consistent results. If `key` is `nil`, then the message is placed
// into a random partition.
//
// Errors usually indicate a catastrophic failure of the Kafka cluster, or
// missing topic if there cluster is not configured to auto create topics.
func (p *T) Produce(topic string, key, message sarama.Encoder, headers []sarama.RecordHeader) (*sarama.ProducerMessage, error) {
	rs := <-p.AsyncProduce(topic, key, message, headers, nil)
	return rs.Msg, rs.Err
}

// AsyncProduce is an asynchronously counterpart of the `Produce` function.
// `token` is the byte budget reservation associated with the message; it is
// released by the dispatcher when the message gets a final acknowledgement, a
// terminal error or the producer is shut down. Pass nil for tokenless
// submissions (tests only).
func (p *T) AsyncProduce(topic string, key, message sarama.Encoder, headers []sarama.RecordHeader, token quotaToken) <-chan Response {
	responseCh := make(chan Response, 1)
	prodMsg := &sarama.ProducerMessage{
		Topic:   topic,
		Key:     key,
		Value:   message,
		Headers: headers,
		Metadata: produceMeta{
			replyCh: responseCh,
			token:   token,
		},
	}
	p.dispatcherCh <- prodMsg
	return responseCh
}

// merge receives both message acknowledgements and producer errors from the
// respective `sarama.AsyncProducer` channels, constructs `ProducerResult`s out
// of them and sends the constructed `ProducerResult` instances to `responseCh`
// to be further inspected by the `dispatcher` goroutine.
//
// It keeps running until both `sarama.AsyncProducer` output channels are
// closed. Then it closes the `responseCh` to notify the `dispatcher` goroutine
// that all pending messages have been processed and exits.
func (p *T) runMerger() {
	nilOrProdSuccessesCh := p.saramaProducer.Successes()
	nilOrProdErrorsCh := p.saramaProducer.Errors()
mergeLoop:
	for channelsOpened := 2; channelsOpened > 0; {
		select {
		case ackedMsg, ok := <-nilOrProdSuccessesCh:
			if !ok {
				channelsOpened -= 1
				nilOrProdSuccessesCh = nil
				continue mergeLoop
			}
			p.responseCh <- Response{Msg: ackedMsg}
		case prodErr, ok := <-nilOrProdErrorsCh:
			if !ok {
				channelsOpened -= 1
				nilOrProdErrorsCh = nil
				continue mergeLoop
			}
			p.responseCh <- Response{Msg: prodErr.Msg, Err: prodErr.Err}
		}
	}
	// Close the result channel to notify the `dispatcher` goroutine that all
	// pending messages have been processed.
	close(p.responseCh)
}

// dispatch implements message processing and graceful shutdown. It receives
// messages from `dispatchedCh` where they are send to by `Produce` method and
// submits them to the embedded `sarama.AsyncProducer`. The dispatcher main
// purpose is to prevent loss of messages during shutdown. It achieves that by
// allowing some graceful period after it stops receiving messages and stopping
// the embedded `sarama.AsyncProducer`.
//
// `pending` keeps the produceMeta (response channel and byte budget
// ownership) of every admitted message. A reservation is released only when
// the corresponding message reaches its terminal state: a broker
// acknowledgement, a terminal producer error, or shutdown for messages that
// never got either.
func (p *T) runDispatcher() {
	nilOrDispatcherCh := p.dispatcherCh
	var nilOrProdInputCh chan<- *sarama.ProducerMessage
	pendingMsgCount := 0
	// The normal operation loop is implemented as two-stroke machine. On the
	// first stroke a message is received from `dispatchCh`, and on the second
	// it is sent to `prodInputCh`. Note that producer results can be received
	// at any time.
	//
	// `pending` keeps the produceMeta (response channel and byte budget
	// ownership) of every admitted message. A reservation is released only
	// when the corresponding message reaches its terminal state: a broker
	// acknowledgement, a terminal producer error, or shutdown for messages
	// that never got either.
	pending := make(map[*sarama.ProducerMessage]produceMeta)
	prodMsg := (*sarama.ProducerMessage)(nil)
	channelOpened := true
	for {
		select {
		case prodMsg, channelOpened = <-nilOrDispatcherCh:
			if !channelOpened {
				goto gracefulShutdown
			}
			pendingMsgCount += 1
			pending[prodMsg] = metadataOf(prodMsg)
			nilOrDispatcherCh = nil
			nilOrProdInputCh = p.saramaProducer.Input()
		case nilOrProdInputCh <- prodMsg:
			nilOrDispatcherCh = p.dispatcherCh
			nilOrProdInputCh = nil
		case prodResult := <-p.responseCh:
			pendingMsgCount -= 1
			p.completeProduceResult(prodResult, pending)
		}
	}
gracefulShutdown:
	// Give the `sarama.AsyncProducer` some time to commit buffered messages.
	//
	// A channel close is observed only once its buffer is drained: receives
	// on a closed buffered channel keep returning the buffered values with
	// ok=true, so by the time we get here every admission taken from the
	// dispatcher channel was also handed over to sarama. Hence the only
	// thing to wait for is terminal acknowledgements for the in-flight
	// messages tracked in `pending`.
	p.dispActDesc.Log().Infof("About to stop producer: pendingMsgCount=%d", pendingMsgCount)
	shutdownTimeoutCh := time.After(p.shutdownTimeout)
	for pendingMsgCount > 0 {
		select {
		case <-shutdownTimeoutCh:
			goto waitForClose
		case prodResult := <-p.responseCh:
			pendingMsgCount -= 1
			p.completeProduceResult(prodResult, pending)
		}
	}
waitForClose:
	// AsyncClose flushes the messages accepted by sarama and delivers a
	// terminal result for each of them; the merger closes responseCh once
	// both sarama output channels are closed.
	p.dispActDesc.Log().Infof("Stopping producer: pendingMsgCount=%d", pendingMsgCount)
	p.saramaProducer.AsyncClose()
	for prodResult := range p.responseCh {
		pendingMsgCount -= 1
		p.completeProduceResult(prodResult, pending)
	}
	// Defensive sweep: a message still tracked here never reached a terminal
	// broker state. Release its byte budget and notify its synchronous
	// caller, so neither quota nor response channels leak on shutdown.
	for msg, meta := range pending {
		delete(pending, msg)
		p.failMessage(msg, meta, ErrShuttingDown)
	}
}

// metadataOf extracts the produceMeta carried by a message.
func metadataOf(msg *sarama.ProducerMessage) produceMeta {
	meta, _ := msg.Metadata.(produceMeta)
	return meta
}

// completeProduceResult handles a terminal producer result: delivers the
// result to the message response channel, releases the byte budget
// reservation and logs/test-reports failures.
func (p *T) completeProduceResult(result Response, pending map[*sarama.ProducerMessage]produceMeta) {
	meta, ok := pending[result.Msg]
	if ok {
		delete(pending, result.Msg)
	} else {
		meta = metadataOf(result.Msg)
	}
	if meta.replyCh != nil {
		meta.replyCh <- result
	}
	if meta.token != nil {
		meta.token.Release()
	}
	if result.Err == nil {
		return
	}
	p.reportFailure(result.Msg, result.Err)
}

// failMessage releases the budget of a message that never reached a terminal
// broker state, reports the failure to its synchronous caller (if any) and
// logs it.
func (p *T) failMessage(msg *sarama.ProducerMessage, meta produceMeta, err error) {
	if meta.replyCh != nil {
		meta.replyCh <- Response{Msg: msg, Err: err}
	}
	if meta.token != nil {
		meta.token.Release()
	}
	p.reportFailure(msg, err)
}

// reportFailure logs a produce failure and, in tests, publishes the dropped
// message to the observation channel.
func (p *T) reportFailure(msg *sarama.ProducerMessage, err error) {
	prodMsgRepr := fmt.Sprintf(`{Topic: "%s", Key: "%s", Value: "%s"}`,
		msg.Topic, encoderRepr(msg.Key), encoderRepr(msg.Value))
	p.dispActDesc.Log().WithError(err).Errorf("Failed to submit message: msg=%v", prodMsgRepr)
	if p.testDroppedMsgCh != nil {
		p.testDroppedMsgCh <- msg
	}
}

// encoderRepr returns the string representation of an encoder value. The value
// is truncated to `maxEncoderReprLength`.
func encoderRepr(e sarama.Encoder) string {
	var repr string
	switch e := e.(type) {
	case sarama.StringEncoder:
		repr = string(e)
	case sarama.ByteEncoder:
		repr = fmt.Sprintf("%X", []byte(e))
	default:
		repr = fmt.Sprint(e)
	}
	if length := len(repr); length > maxEncoderReprLength {
		repr = fmt.Sprintf("%s... (%d bytes more)",
			repr[:maxEncoderReprLength], length-maxEncoderReprLength)
	}
	return repr
}
