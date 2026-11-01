package proxy

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/admin"
	"github.com/mailgun/kafka-pixy/budget"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/consumerimpl"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/mailgun/kafka-pixy/producer"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	initEventsChMapCapacity = 256

	// recordFramingOverhead estimates the fixed per-record Kafka wire
	// overhead (CRC, flags, timestamp, lengths, ...). It mirrors the
	// accounting historically used to derive the admittable value size from
	// producer.max_message_bytes.
	recordFramingOverhead = 26
	// recordHeaderOverhead estimates the per-header length prefixes on top of
	// the header key and value bytes.
	recordHeaderOverhead = 4
)

var (
	ErrUnavailable        = errors.New("service is shutting down")
	ErrDisabled           = errors.New("service is disabled by configuration")
	ErrHeadersUnsupported = errors.New("headers are not supported with this version of Kafka. Consider changing `kafka.version` (https://github.com/mailgun/kafka-pixy/blob/master/default.yaml#L35)")
	// ErrMessageTooLarge is returned before the payload is read/enqueued when
	// it exceeds producer.max_message_bytes.
	ErrMessageTooLarge = errors.New("message is too large")
	// ErrQuotaExhausted is returned at admission time when the produce byte
	// budget has no room for the request. Callers map it to HTTP 429 / gRPC
	// ResourceExhausted.
	ErrQuotaExhausted = budget.ErrQuotaExhausted

	noAck   = Ack{partition: -1}
	autoAck = Ack{partition: -2}
)

// T implements a proxy to a particular Kafka/ZooKeeper cluster.
type T struct {
	actDesc    *actor.Descriptor
	cfg        *config.Proxy
	kafkaClt   sarama.Client
	offsetMgrF offsetmgr.Factory

	adminMu sync.RWMutex
	admin   *admin.T

	producerMu sync.RWMutex
	producer   *producer.T

	// lane pairs the process-global produce byte limiter with the per-cluster
	// one; it is attached by Set.NewSet once all proxies are spawned.
	lane budget.Lane

	consumerMu sync.RWMutex
	consumer   consumer.T

	// FIXME: We never remove stale elements from eventsChMap. It is sort of ok
	// FIXME: since the number of group/topic/partition combinations is fairly
	// FIXME: limited and should not cause any significant system memory usage.
	eventsChMapMu sync.RWMutex
	eventsChMap   map[eventsChID]chan<- consumer.Event
}

type Ack struct {
	partition int32
	offset    int64
}

// NewAck creates an acknowledgement instance from a partition and an offset.
// Note that group and topic are not included. Respective values that are
// passed to proxy.Consume function along with the ack are gonna be used.
func NewAck(partition int32, offset int64) (Ack, error) {
	if partition < 0 {
		return Ack{}, errors.Errorf("bad partition: %d", partition)
	}
	if offset < 0 {
		return Ack{}, errors.Errorf("bad offset: %d", offset)
	}
	return Ack{partition, offset}, nil
}

// NoAck returns an ack value that should be passed to proxy.Consume function
// when a caller does not want to acknowledge anything.
func NoAck() Ack {
	return noAck
}

// AutoAck returns an ack value that should be passed to proxy.Consume function
// when a caller wants the consumed message to be acknowledged immediately.
func AutoAck() Ack {
	return autoAck
}

type eventsChID struct {
	group     string
	topic     string
	partition int32
}

// Spawn creates a proxy instance and starts its internal goroutines.
func Spawn(parentActDesc *actor.Descriptor, name string, cfg *config.Proxy) (*T, error) {
	p := T{
		actDesc:     parentActDesc.NewChild(name),
		cfg:         cfg,
		eventsChMap: make(map[eventsChID]chan<- consumer.Event, initEventsChMapCapacity),
	}
	var err error

	if p.kafkaClt, err = sarama.NewClient(cfg.Kafka.SeedPeers, cfg.SaramaClientCfg()); err != nil {
		return nil, errors.Wrap(err, "failed to create Kafka client")
	}
	p.offsetMgrF = offsetmgr.SpawnFactory(p.actDesc, cfg, p.kafkaClt)
	if p.producer, err = producer.Spawn(p.actDesc, cfg); err != nil {
		return nil, errors.Wrap(err, "failed to spawn producer")
	}
	if !cfg.Consumer.Disabled {
		if p.consumer, err = consumerimpl.Spawn(p.actDesc, cfg, p.offsetMgrF); err != nil {
			return nil, errors.Wrap(err, "failed to spawn consumer")
		}
	}
	if p.admin, err = admin.Spawn(p.actDesc, cfg); err != nil {
		return nil, errors.Wrap(err, "failed to spawn admin")
	}
	return &p, nil
}

// Stop terminates the proxy instances synchronously.
func (p *T) Stop() {
	var wg sync.WaitGroup

	p.producerMu.RLock()
	if p.producer != nil {
		actor.Spawn(p.actDesc.NewChild("prod_stop"), &wg, p.stopProducer)
	}
	p.producerMu.RUnlock()

	p.consumerMu.RLock()
	if p.consumer != nil {
		actor.Spawn(p.actDesc.NewChild("cons_stop"), &wg, p.stopConsumer)
	}
	p.consumerMu.RUnlock()

	p.adminMu.RLock()
	if p.admin != nil {
		actor.Spawn(p.actDesc.NewChild("adm_stop"), &wg, p.stopAdmin)
	}
	p.adminMu.RUnlock()

	wg.Wait()
	if p.offsetMgrF != nil {
		p.offsetMgrF.Stop()
	}
	if p.kafkaClt != nil {
		p.kafkaClt.Close()
	}
}

func (p *T) stopConsumer() {
	p.consumerMu.Lock()
	cons := p.consumer
	p.consumer = nil
	p.consumerMu.Unlock()
	if cons != nil {
		cons.Stop()
	}
}

func (p *T) stopProducer() {
	p.producerMu.Lock()
	prod := p.producer
	p.producer = nil
	p.producerMu.Unlock()
	prod.Stop()
}

func (p *T) stopAdmin() {
	p.adminMu.Lock()
	p.admin.Stop()
	p.adminMu.Unlock()
}

// PayloadSpec describes the billable parts of a produce request at
// admission time, before (or independently from) reading the message body.
// Billing explicitly covers the key, the value, the decoded Kafka headers
// and fixed record framing — it is never derived from Content-Length alone.
type PayloadSpec struct {
	// KeyLength is the number of bytes of the message key (0 for no key).
	KeyLength int
	// ValueLength is the declared value size in bytes. A negative value
	// means the length is not known yet (chunked HTTP upload); the maximum
	// admittable value size is reserved until the actual length is committed.
	ValueLength int
	// Headers are the decoded Kafka record headers; both key and value bytes
	// are accounted.
	Headers []sarama.RecordHeader
}

// Admission is a byte budget reservation granted for one produce request.
// Ownership flows from the request handler to the producer dispatcher, which
// releases it when the message gets a final broker acknowledgement, a
// terminal failure, or the producer is stopped. If the request fails before
// being submitted (bad body, oversize, etc.) the caller must Release it.
type Admission struct {
	p *T

	reservation *budget.Reservation
	// fixedBytes is the accounted weight components other than the value:
	// key, headers and record framing, after the duplication factor.
	fixedWeight int64
	// valueReserved is the value length the reservation was created with.
	valueReserved int
	// valueLimit is the maximum admittable value length for this request.
	valueLimit int
}

// AdmitProduce performs all pre-enqueue checks and, when they pass, reserves
// the byte weight of the request in both the process-global and the
// per-cluster produce budgets. It does not block: a lack of quota is
// reported as ErrQuotaExhausted so callers can return HTTP 429 / gRPC
// ResourceExhausted.
func (p *T) AdmitProduce(spec PayloadSpec) (*Admission, error) {
	p.producerMu.RLock()
	stopped := p.producer == nil
	p.producerMu.RUnlock()
	if stopped {
		return nil, ErrUnavailable
	}

	if len(spec.Headers) > 0 && !p.cfg.Kafka.Version.IsAtLeast(sarama.V0_11_0_0) {
		return nil, ErrHeadersUnsupported
	}

	headersLen := 0
	for _, h := range spec.Headers {
		headersLen += len(h.Key) + len(h.Value)
	}
	overhead := recordFramingOverhead + len(spec.Headers)*recordHeaderOverhead
	valueLimit := p.cfg.Producer.MaxMessageBytes - overhead - spec.KeyLength - headersLen
	if valueLimit < 0 {
		return nil, ErrMessageTooLarge
	}

	declared := spec.ValueLength
	if declared < 0 {
		// Unknown declared length (chunked transfer): reserve the worst case
		// and shrink the reservation once the actual size is known.
		declared = valueLimit
	}
	if declared > valueLimit {
		return nil, ErrMessageTooLarge
	}

	payloadBytes := int64(overhead + spec.KeyLength + headersLen + declared)
	weight := p.lane.Weight(payloadBytes)
	reservation, err := p.lane.TryAcquire(weight)
	if err != nil {
		return nil, ErrQuotaExhausted
	}
	return &Admission{
		p:             p,
		reservation:   reservation,
		fixedWeight:   p.lane.Weight(int64(overhead + spec.KeyLength + headersLen)),
		valueReserved: declared,
		valueLimit:    valueLimit,
	}, nil
}

// MaxValueLength is the maximum number of value bytes allowed for this
// admitted request; HTTP handlers use it to cap the bounded body read.
func (a *Admission) MaxValueLength() int {
	return a.valueLimit
}

// CommitValue reconciles the reservation with the actual value size. It
// rejects oversize values and returns any over-reserved bytes (unknown
// declared length or form-encoded bodies). The returned error, if any, has
// already released the admission.
func (a *Admission) CommitValue(actualLength int) error {
	if actualLength > a.valueLimit {
		a.Release()
		return ErrMessageTooLarge
	}
	if actualLength < a.valueReserved {
		a.reservation.Shrink(a.fixedWeight + a.p.lane.Weight(int64(actualLength)))
		a.valueReserved = actualLength
	}
	return nil
}

// Release returns the reserved bytes. It is idempotent and must be called by
// the admitting request when submission never happens; after a successful
// submission the producer dispatcher owns the reservation.
func (a *Admission) Release() {
	a.reservation.Release()
}

// Produce submits an admitted message synchronously. On success the returned
// ProducerMessage carries the partition and offset assigned by the broker;
// that partition follows the existing key-partitioning rules. Pre-enqueue
// failures (shutdown) are returned immediately; post-enqueue failures come
// from the broker and may be e.g. sarama.ErrUnknownTopicOrPartition.
func (p *T) Produce(a *Admission, topic string, key, message sarama.Encoder, headers []sarama.RecordHeader) (*sarama.ProducerMessage, error) {
	responseCh, err := p.submit(a, topic, key, message, headers)
	if err != nil {
		return nil, err
	}
	rs := <-responseCh
	if rs.Err == producer.ErrShuttingDown {
		return nil, ErrUnavailable
	}
	return rs.Msg, rs.Err
}

// AsyncProduce submits an admitted message asynchronously. A nil return only
// means that the request holds byte budget and has entered the proxy produce
// pipeline; it does not promise the eventual broker write.
func (p *T) AsyncProduce(a *Admission, topic string, key, message sarama.Encoder, headers []sarama.RecordHeader) error {
	_, err := p.submit(a, topic, key, message, headers)
	return err
}

// submit enqueues the admitted message to the producer, transferring budget
// ownership to the producer dispatcher. The read lock is held across the
// enqueue so that producer shutdown cannot close the dispatcher channel
// while the message is being sent.
func (p *T) submit(a *Admission, topic string, key, message sarama.Encoder, headers []sarama.RecordHeader) (<-chan producer.Response, error) {
	p.producerMu.RLock()
	defer p.producerMu.RUnlock()
	if p.producer == nil {
		a.Release()
		return nil, ErrUnavailable
	}
	return p.producer.AsyncProduce(topic, key, message, headers, a.reservation), nil
}

// setAdmission attaches the process-global/per-cluster admission lane. It is
// called by Set while wiring spawned proxies.
func (p *T) setAdmission(lane budget.Lane) {
	p.lane = lane
}

// Consume consumes a message from the specified topic on behalf of the
// specified consumer group. If there are no more new messages in the topic
// at the time of the request then it will block for
// `Config.Consumer.LongPollingTimeout`. If no new message is produced during
// that time, then `ErrRequestTimeout` is returned.
//
// Note that during state transitions topic subscribe<->unsubscribe and
// consumer group register<->deregister the method may return either
// `ErrBufferOverflow` or `ErrRequestTimeout` even when there are messages
// available for consumption. In that case the user should back off a bit
// and then repeat the request.
func (p *T) Consume(group, topic string, ack Ack) (consumer.Message, error) {
	if p.cfg.Consumer.Disabled {
		return consumer.Message{}, ErrDisabled
	}

	if ack != noAck && ack != autoAck {
		p.eventsChMapMu.RLock()
		eventsChID := eventsChID{group, topic, ack.partition}
		eventsCh, ok := p.eventsChMap[eventsChID]
		p.eventsChMapMu.RUnlock()
		if ok {
			go func() {
				select {
				case eventsCh <- consumer.Ack(ack.offset):
				case <-time.After(p.cfg.Consumer.LongPollingTimeout):
					p.actDesc.Log().WithFields(log.Fields{
						"kafka.group":     group,
						"kafka.topic":     topic,
						"kafka.partition": ack.partition,
					}).Errorf("ack timeout: offset=%d", ack.offset)
				}
			}()
		}
	}

	p.consumerMu.RLock()
	if p.consumer == nil {
		p.consumerMu.RUnlock()
		return consumer.Message{}, ErrUnavailable
	}
	responseCh := p.consumer.AsyncConsume(group, topic)
	p.consumerMu.RUnlock()

	rs := <-responseCh
	if rs.Err != nil {
		return consumer.Message{}, rs.Err
	}

	eventsChID := eventsChID{group, topic, rs.Msg.Partition}
	p.eventsChMapMu.Lock()
	p.eventsChMap[eventsChID] = rs.Msg.EventsCh
	p.eventsChMapMu.Unlock()

	if ack == autoAck {
		rs.Msg.EventsCh <- consumer.Ack(rs.Msg.Offset)
	}
	return rs.Msg, nil
}

func (p *T) Ack(group, topic string, ack Ack) error {
	eventsChID := eventsChID{group, topic, ack.partition}
	p.eventsChMapMu.RLock()
	eventsCh, ok := p.eventsChMap[eventsChID]
	p.eventsChMapMu.RUnlock()
	if !ok {
		return errors.Errorf("acks channel missing for %v", eventsChID)
	}
	select {
	case eventsCh <- consumer.Ack(ack.offset):
	case <-time.After(p.cfg.Consumer.LongPollingTimeout):
		return errors.New("ack timeout")
	}
	return nil
}

// GetGroupOffsets for every partition of the specified topic it returns the
// current offset range along with the latest offset and metadata committed by
// the specified consumer group.
func (p *T) GetGroupOffsets(group, topic string) ([]admin.PartitionOffset, error) {
	p.adminMu.RLock()
	defer p.adminMu.RUnlock()
	if p.admin == nil {
		return nil, ErrUnavailable
	}
	return p.admin.GetGroupOffsets(group, topic)
}

// SetGroupOffsets commits specific offset values along with metadata for a list
// of partitions of a particular topic on behalf of the specified group.
func (p *T) SetGroupOffsets(group, topic string, offsets []admin.PartitionOffset) error {
	p.adminMu.RLock()
	defer p.adminMu.RUnlock()
	if p.admin == nil {
		return ErrUnavailable
	}
	return p.admin.SetGroupOffsets(group, topic, offsets)
}

// GetTopicConsumers returns client-id -> consumed-partitions-list mapping
// for a clients from a particular consumer group and a particular topic.
func (p *T) GetTopicConsumers(group, topic string) (map[string][]int32, error) {
	p.adminMu.RLock()
	defer p.adminMu.RUnlock()
	if p.admin == nil {
		return nil, ErrUnavailable
	}
	return p.admin.GetTopicConsumers(group, topic)
}

// GetAllTopicConsumers returns group -> client-id -> consumed-partitions-list
// mapping for a particular topic. Warning, the function performs scan of all
// consumer groups registered in ZooKeeper and therefore can take a lot of time.
func (p *T) GetAllTopicConsumers(topic string) (map[string]map[string][]int32, error) {
	p.adminMu.RLock()
	defer p.adminMu.RUnlock()
	if p.admin == nil {
		return nil, ErrUnavailable
	}
	return p.admin.GetAllTopicConsumers(topic)
}

// ListTopics returns a list of all topics existing in the Kafka cluster.
func (p *T) ListTopics(withPartitions, withConfig bool) ([]admin.TopicMetadata, error) {
	p.adminMu.RLock()
	defer p.adminMu.RUnlock()
	if p.admin == nil {
		return nil, ErrUnavailable
	}
	return p.admin.ListTopics(withPartitions, withConfig)
}

// GetTopicMetadata returns a topic metadata. An optional partition metadata
// can be requested and/or detailed topic configuration can be requested.
func (p *T) GetTopicMetadata(topic string, withPartitions, withConfig bool) (admin.TopicMetadata, error) {
	p.adminMu.RLock()
	defer p.adminMu.RUnlock()
	if p.admin == nil {
		return admin.TopicMetadata{}, ErrUnavailable
	}
	return p.admin.GetTopicMetadata(topic, withPartitions, withConfig)
}
