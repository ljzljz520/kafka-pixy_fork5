package httpsrv

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/gorilla/mux"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/admin"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/offsettrk"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/mailgun/kafka-pixy/prettyfmt"
	"github.com/mailgun/kafka-pixy/proxy"
	"github.com/pkg/errors"
)

const (
	networkTCP  = "tcp"
	networkUnix = "unix"

	// HTTP headers used by the API.
	hdrContentLength = "Content-Length"
	hdrContentType   = "Content-Type"
	hdrKafkaPrefix   = "X-Kafka-"

	// HTTP request parameters.
	prmCluster              = "cluster"
	prmTopic                = "topic"
	prmKey                  = "key"
	prmSync                 = "sync"
	prmGroup                = "group"
	prmNoAck                = "noAck"
	prmAckPartition         = "ackPartition"
	prmPartition            = "partition"
	prmAckOffset            = "ackOffset"
	prmOffset               = "offset"
	prmTopicsWithPartitions = "withPartitions"
	prmTopicsWithConfig     = "withConfig"
)

var (
	EmptyResponse          = map[string]interface{}{}
	jsonContentTypePattern *regexp.Regexp
)

type T struct {
	actDesc    *actor.Descriptor
	addr       string
	listener   net.Listener
	httpServer *http.Server
	proxySet   *proxy.Set
	wg         sync.WaitGroup
	errorCh    chan error

	// needed to pass in security info
	certPath string
	keyPath  string
}

func init() {
	var err error
	if jsonContentTypePattern, err = regexp.Compile("^application/(?:.*\\+)?json(?:;.*)?$"); err != nil {
		panic(err)
	}
}

// New creates an HTTP server instance that will accept API requests at the
// specified `network`/`address` and execute them with the specified `producer`,
// `consumer`, or `admin`, depending on the request type.
//
// It also passes in the provided certificate and key paths for TLS. If
// empty strings, it is run in non-TLS mode.
func New(addr string, proxySet *proxy.Set, certPath, keyPath string) (*T, error) {
	network := networkUnix
	if strings.Contains(addr, ":") {
		network = networkTCP
	}
	// Start listening on the specified network/address.
	listener, err := net.Listen(network, addr)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create listener")
	}
	// If the address is Unix Domain Socket then make it accessible for everyone.
	if network == networkUnix {
		if err := os.Chmod(addr, 0777); err != nil {
			return nil, errors.Wrap(err, "failed to change socket permissions")
		}
	}
	// Create a graceful HTTP server instance.
	router := mux.NewRouter()
	httpServer := &http.Server{Handler: router}

	hs := &T{
		actDesc:    actor.Root().NewChild(addr),
		addr:       addr,
		listener:   listener,
		httpServer: httpServer,
		proxySet:   proxySet,
		errorCh:    make(chan error, 1),
		certPath:   certPath,
		keyPath:    keyPath,
	}
	// Configure the API request handlers.
	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics/{%s}/messages", prmCluster, prmTopic), hs.handleProduce).Methods("POST")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/messages", prmTopic), hs.handleProduce).Methods("POST")

	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics/{%s}/messages", prmCluster, prmTopic), hs.handleConsume).Methods("GET")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/messages", prmTopic), hs.handleConsume).Methods("GET")

	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics/{%s}/acks", prmCluster, prmTopic), hs.handleAck).Methods("POST")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/acks", prmTopic), hs.handleAck).Methods("POST")

	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics/{%s}/offsets", prmCluster, prmTopic), hs.handleGetOffsets).Methods("GET")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/offsets", prmTopic), hs.handleGetOffsets).Methods("GET")

	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics/{%s}/offsets", prmCluster, prmTopic), hs.handleSetOffsets).Methods("POST")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/offsets", prmTopic), hs.handleSetOffsets).Methods("POST")

	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics/{%s}/consumers", prmCluster, prmTopic), hs.handleGetTopicConsumers).Methods("GET")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/consumers", prmTopic), hs.handleGetTopicConsumers).Methods("GET")

	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics", prmCluster), hs.handleListTopics).Methods("GET")
	router.HandleFunc("/topics", hs.handleListTopics).Methods("GET")

	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics/{%s}", prmCluster, prmTopic), hs.handleGetTopicMetadata).Methods("GET")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}", prmTopic), hs.handleGetTopicMetadata).Methods("GET")

	router.HandleFunc("/pprof/heap", http.HandlerFunc(getHeapProfile)).Methods("GET")

	router.HandleFunc("/_ping", hs.handlePing).Methods("GET")
	return hs, nil
}

// Start starts triggers asynchronous HTTP server start. If it fails then the error
// will be sent down to `ErrorCh()`.
func (s *T) Start() {
	actor.Spawn(s.actDesc, &s.wg, func() {
		if s.certPath != "" && s.keyPath != "" {
			if err := s.httpServer.ServeTLS(s.listener, s.certPath, s.keyPath); err != nil {
				s.errorCh <- errors.Wrap(err, "HTTP API server failed")
			}
		} else {
			if err := s.httpServer.Serve(s.listener); err != nil {
				s.errorCh <- errors.Wrap(err, "HTTP API server failed")
			}
		}
	})
}

// ErrorCh returns an output channel that HTTP server running in another
// goroutine will use if it stops with error if one occurs. The channel will be
// closed when the server is fully stopped due to an error or otherwise..
func (s *T) ErrorCh() <-chan error {
	return s.errorCh
}

// Stop gracefully stops the HTTP API server. It stops listening on the socket
// for incoming requests first, and then blocks waiting for pending requests to
// complete.
func (s *T) Stop() {
	s.httpServer.Shutdown(context.Background())
	s.wg.Wait()
	close(s.errorCh)
}

func (s *T) getProxy(r *http.Request) (*proxy.T, error) {
	cluster := mux.Vars(r)[prmCluster]
	return s.proxySet.Get(cluster)
}

// handleProduce is an HTTP request handler for `POST /topic/{topic}/messages`.
//
// The handler implements a byte-metered admission contract:
//  1. Query parameters and X-Kafka headers are validated without touching the
//     request body.
//  2. A staging slot is taken and the byte weight (key + declared value +
//     decoded headers + record framing, with the duplication margin) is
//     reserved in the proxy before the body is read.
//  3. The body is read through http.MaxBytesReader, so an oversized or
//     maliciously slow client can never buffer more than the admitted bytes.
//  4. On success ownership of the reservation is transferred to the producer;
//     it is released only when the message gets a final broker
//     acknowledgement, a terminal failure, or the producer shuts down.
func (s *T) handleProduce(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	pxy, err := s.getProxy(r)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	topic := mux.Vars(r)[prmTopic]

	// Parse query parameters; r.URL.Query() does not read the request body,
	// unlike r.ParseForm()/r.FormValue().
	query := r.URL.Query()
	key := queryParamBytes(query, prmKey)
	_, isSync := query[prmSync]
	contentType := r.Header.Get(hdrContentType)

	if !isSupportedProduceContentType(contentType) {
		s.respondWithJSON(w, http.StatusBadRequest,
			errorRs{fmt.Sprintf("unsupported content type %s", contentType)})
		return
	}

	// Raw (text/json) requests must declare an exact Content-Length; this
	// allows reserving the exact weight before reading any body byte.
	declaredLen := int(r.ContentLength)
	if isRawProduceContentType(contentType) {
		messageSizeStr, ok := r.Header[hdrContentLength]
		if !ok {
			s.respondWithJSON(w, http.StatusBadRequest,
				errorRs{fmt.Sprintf("missing %s header", hdrContentLength)})
			return
		}
		declaredLen, err = strconv.Atoi(messageSizeStr[0])
		if err != nil {
			s.respondWithJSON(w, http.StatusBadRequest,
				errorRs{fmt.Sprintf("invalid %s header: %s", hdrContentLength, messageSizeStr[0])})
			return
		}
		if declaredLen < 0 {
			s.respondWithJSON(w, http.StatusBadRequest,
				errorRs{fmt.Sprintf("invalid %s header: %s", hdrContentLength, messageSizeStr[0])})
			return
		}
	}

	// Bound the number of requests that may hold parsed/staged bytes (decoded
	// headers, reserved worst-case values) before acquiring a formal byte
	// reservation. The slot is released as soon as admission completes; the
	// byte reservation itself covers the bounded body read and the pipeline.
	if !s.proxySet.TryAcquireProduceSlot() {
		s.respondWithJSON(w, http.StatusTooManyRequests,
			errorRs{"too many concurrent produce requests"})
		return
	}

	// Decode X-Kafka headers before admission, so header bytes are billed and
	// malformed values are rejected without reserving anything.
	headers, err := decodeKafkaHeaders(r.Header)
	if err != nil {
		s.proxySet.ReleaseProduceSlot()
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	// For raw bodies the declared Content-Length is exactly the value size.
	// For urlencoded bodies Content-Length is the wire size (percent encoding
	// may inflate every value byte up to 3x), so the decoded value length is
	// unknown at this point and the worst case is reserved, then shrunk once
	// the form is parsed.
	spec := proxy.PayloadSpec{
		KeyLength:   len(key),
		ValueLength: -1,
		Headers:     headers,
	}
	if isRawProduceContentType(contentType) {
		spec.ValueLength = declaredLen
	}
	admission, err := pxy.AdmitProduce(spec)
	s.proxySet.ReleaseProduceSlot()
	if err != nil {
		s.respondWithJSON(w, produceErrorStatus(err), errorRs{err.Error()})
		return
	}
	submitted := false
	defer func() {
		if !submitted {
			admission.Release()
		}
	}()

	// Bounded read happens with the reservation already held. Raw bodies are
	// capped at one byte past the admitted value. Urlencoded bodies may
	// percent-encode every byte (up to 3 wire bytes per value byte) plus the
	// field name and a small fixed margin; a declared Content-Length, when
	// present, tightens the cap further. That wire expansion is a bounded
	// transport buffer margin and is returned via Shrink once the value is
	// parsed.
	maxValueLen := int64(admission.MaxValueLength())
	var readLimit int64
	if isRawProduceContentType(contentType) {
		readLimit = maxValueLen + 1
	} else {
		readLimit = 3*maxValueLen + formBodyOverhead + 1
		if declaredLen >= 0 && int64(declaredLen)+1 < readLimit {
			readLimit = int64(declaredLen) + 1
		}
	}
	body, err := s.readProduceBody(w, r, readLimit)
	if err != nil {
		if errors.Cause(err) == errBodyTooLarge {
			s.respondWithJSON(w, http.StatusRequestEntityTooLarge, errorRs{err.Error()})
			return
		}
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	var value sarama.Encoder
	if isRawProduceContentType(contentType) {
		if len(body) != declaredLen {
			s.respondWithJSON(w, http.StatusBadRequest, errorRs{
				fmt.Sprintf("message size does not match %s: expected=%v, actual=%v",
					hdrContentLength, declaredLen, len(body))})
			return
		}
		if err := admission.CommitValue(len(body)); err != nil {
			s.respondWithJSON(w, produceErrorStatus(err), errorRs{err.Error()})
			return
		}
		value = sarama.ByteEncoder(body)
	} else {
		// Parse the urlencoded form from the already bounded body bytes.
		r.Body = ioutil.NopCloser(bytes.NewReader(body))
		if err := r.ParseForm(); err != nil {
			s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
			return
		}
		msg := r.FormValue("msg")
		if msg == "" {
			s.respondWithJSON(w, http.StatusBadRequest, errorRs{"empty message"})
			return
		}
		if err := admission.CommitValue(len(msg)); err != nil {
			s.respondWithJSON(w, produceErrorStatus(err), errorRs{err.Error()})
			return
		}
		value = sarama.StringEncoder(msg)
	}

	// Asynchronously submit the message to the Kafka cluster. A nil error
	// only means that the request was admitted and entered the pipeline.
	if !isSync {
		if err = pxy.AsyncProduce(admission, topic, toEncoderPreservingNil(key), value, headers); err != nil {
			s.respondWithJSON(w, produceErrorStatus(err), errorRs{err.Error()})
			return
		}
		submitted = true
		s.respondWithJSON(w, http.StatusOK, EmptyResponse)
		return
	}

	prodMsg, err := pxy.Produce(admission, topic, toEncoderPreservingNil(key), value, headers)
	if err != nil {
		s.respondWithJSON(w, produceSyncErrorStatus(err), errorRs{err.Error()})
		return
	}
	submitted = true
	s.respondWithJSON(w, http.StatusOK, produceRs{
		Partition: prodMsg.Partition,
		Offset:    prodMsg.Offset,
	})
}

// errBodyTooLarge is returned when a bounded produce body read exceeds the
// admitted size.
var errBodyTooLarge = errors.New("message body is too large")

// formBodyOverhead bounds the non-value wire bytes of an urlencoded produce
// request: the "msg=" field name, delimiters and a small margin for any
// additional field.
const formBodyOverhead = 64

// readProduceBody reads the request body bounded by `limit` bytes. It never
// buffers more than the limit and returns errBodyTooLarge when the body is
// larger.
func (s *T) readProduceBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, error) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		if isRequestBodyTooLarge(err) {
			return nil, errBodyTooLarge
		}
		return nil, errors.Wrap(err, "failed to read message")
	}
	return body, nil
}

// isRequestBodyTooLarge recognizes the error of http.MaxBytesReader. Since
// Go 1.19 it is the exported *http.MaxBytesError; on earlier toolchains it
// is an unexported error carrying the same stable message.
func isRequestBodyTooLarge(err error) bool {
	return err != nil && strings.Contains(err.Error(), "http: request body too large")
}

// isSupportedProduceContentType reports whether the content type can carry a
// produce request.
func isSupportedProduceContentType(contentType string) bool {
	return isRawProduceContentType(contentType) ||
		contentType == "application/x-www-form-urlencoded"
}

// isRawProduceContentType reports whether the body bytes are the message
// value verbatim (text/plain and JSON variants).
func isRawProduceContentType(contentType string) bool {
	return contentType == "text/plain" || jsonContentTypePattern.MatchString(contentType)
}

// decodeKafkaHeaders extracts Kafka record headers from X-Kafka-* HTTP
// headers; header values must be standard base64 encoded.
func decodeKafkaHeaders(header http.Header) ([]sarama.RecordHeader, error) {
	var headers []sarama.RecordHeader
	for headerName, values := range header {
		if !strings.HasPrefix(headerName, hdrKafkaPrefix) {
			continue
		}
		headerBytes := []byte(headerName[len(hdrKafkaPrefix):])
		for _, v := range values {
			decoded, err := base64.StdEncoding.DecodeString(v)
			if err != nil {
				return nil, errors.Errorf("Invalid base64 encoding for header: %s", headerName)
			}
			headers = append(headers, sarama.RecordHeader{
				Key:   headerBytes,
				Value: decoded,
			})
		}
	}
	return headers, nil
}

// produceErrorStatus maps pre-enqueue admission errors to HTTP status codes.
func produceErrorStatus(err error) int {
	switch err {
	case proxy.ErrMessageTooLarge:
		return http.StatusRequestEntityTooLarge
	case proxy.ErrQuotaExhausted:
		return http.StatusTooManyRequests
	case proxy.ErrHeadersUnsupported:
		return http.StatusBadRequest
	case proxy.ErrDisabled, proxy.ErrUnavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// produceSyncErrorStatus maps synchronous produce errors to HTTP status
// codes, preserving the existing post-enqueue error mapping.
func produceSyncErrorStatus(err error) int {
	if err == sarama.ErrUnknownTopicOrPartition {
		return http.StatusNotFound
	}
	return produceErrorStatus(err)
}

// handleConsume is an HTTP request handler for `GET /topic/{topic}/messages`
func (s *T) handleConsume(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	pxy, err := s.getProxy(r)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	topic := mux.Vars(r)[prmTopic]
	group, err := getGroupParam(r, false)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	ack, err := parseAck(r, true)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	consMsg, err := pxy.Consume(group, topic, ack)
	if err != nil {
		var status int
		switch err {
		case consumer.ErrRequestTimeout:
			status = http.StatusRequestTimeout
		case consumer.ErrTooManyRequests:
			status = http.StatusTooManyRequests
		case consumer.ErrUnavailable:
			fallthrough
		case proxy.ErrDisabled:
			fallthrough
		case proxy.ErrUnavailable:
			status = http.StatusServiceUnavailable
		default:
			status = http.StatusInternalServerError
		}
		s.respondWithJSON(w, status, errorRs{err.Error()})
		return
	}

	headers := make([]consumeHeader, 0, len(consMsg.Headers))
	for _, h := range consMsg.Headers {
		headers = append(headers, consumeHeader{
			Key:   string(h.Key),
			Value: h.Value,
		})
	}

	s.respondWithJSON(w, http.StatusOK, consumeRs{
		Key:       consMsg.Key,
		Value:     consMsg.Value,
		Partition: consMsg.Partition,
		Offset:    consMsg.Offset,
		Headers:   headers,
	})
}

// handleConsume is an HTTP request handler for `GET /topic/{topic}/messages`
func (s *T) handleAck(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	pxy, err := s.getProxy(r)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	topic := mux.Vars(r)[prmTopic]
	group, err := getGroupParam(r, false)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	ack, err := parseAck(r, false)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	err = pxy.Ack(group, topic, ack)
	if err != nil {
		s.respondWithJSON(w, http.StatusInternalServerError, errorRs{err.Error()})
		return
	}
	s.respondWithJSON(w, http.StatusOK, EmptyResponse)
}

// handleGetOffsets is an HTTP request handler for `GET /topic/{topic}/offsets`
func (s *T) handleGetOffsets(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	pxy, err := s.getProxy(r)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	topic := mux.Vars(r)[prmTopic]
	group, err := getGroupParam(r, false)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	partitionOffsets, err := pxy.GetGroupOffsets(group, topic)
	if err != nil {
		if errors.Cause(err) == sarama.ErrUnknownTopicOrPartition {
			s.respondWithJSON(w, http.StatusNotFound, errorRs{"Unknown topic"})
			return
		}
		s.respondWithJSON(w, http.StatusInternalServerError, errorRs{err.Error()})
		return
	}

	offsetViews := make([]partitionInfo, len(partitionOffsets))
	for i, po := range partitionOffsets {
		offsetViews[i].Partition = po.Partition
		offsetViews[i].Begin = po.Begin
		offsetViews[i].End = po.End
		offsetViews[i].Count = po.End - po.Begin
		offsetViews[i].Offset = po.Offset
		if po.Offset == sarama.OffsetNewest {
			offsetViews[i].Lag = 0
		} else if po.Offset == sarama.OffsetOldest {
			offsetViews[i].Lag = po.End - po.Begin
		} else {
			offsetViews[i].Lag = po.End - po.Offset
		}
		offsetViews[i].Metadata = po.Metadata
		offset := offsetmgr.Offset{Val: po.Offset, Meta: po.Metadata}
		offsetViews[i].SparseAcks = offsettrk.SparseAcks2Str(offset)
	}
	s.respondWithJSON(w, http.StatusOK, offsetViews)
}

// handleGetOffsets is an HTTP request handler for `POST /topic/{topic}/offsets`
func (s *T) handleSetOffsets(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	pxy, err := s.getProxy(r)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	topic := mux.Vars(r)[prmTopic]
	group, err := getGroupParam(r, false)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		errorText := fmt.Sprintf("Failed to read the request: err=(%s)", err)
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{errorText})
		return
	}

	var partitionOffsetViews []partitionInfo
	if err := json.Unmarshal(body, &partitionOffsetViews); err != nil {
		errorText := fmt.Sprintf("Failed to parse the request: err=(%s)", err)
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{errorText})
		return
	}

	partitionOffsets := make([]admin.PartitionOffset, len(partitionOffsetViews))
	for i, pov := range partitionOffsetViews {
		partitionOffsets[i].Partition = pov.Partition
		partitionOffsets[i].Offset = pov.Offset
		partitionOffsets[i].Metadata = pov.Metadata
	}

	err = pxy.SetGroupOffsets(group, topic, partitionOffsets)
	if err != nil {
		if err = errors.Cause(err); err == sarama.ErrUnknownTopicOrPartition {
			s.respondWithJSON(w, http.StatusNotFound, errorRs{"Unknown topic"})
			return
		}
		s.respondWithJSON(w, http.StatusInternalServerError, errorRs{err.Error()})
		return
	}

	s.respondWithJSON(w, http.StatusOK, EmptyResponse)
}

// handleGetTopicConsumers is an HTTP request handler for `GET /topic/{topic}/consumers`
func (s *T) handleGetTopicConsumers(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var err error

	pxy, err := s.getProxy(r)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	topic := mux.Vars(r)[prmTopic]

	group, err := getGroupParam(r, true)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	var consumers map[string]map[string][]int32
	if group == "" {
		consumers, err = pxy.GetAllTopicConsumers(topic)
		if err != nil {
			s.respondWithJSON(w, http.StatusInternalServerError, errorRs{err.Error()})
			return
		}
	} else {
		groupConsumers, err := pxy.GetTopicConsumers(group, topic)
		if err != nil {
			if _, ok := err.(admin.ErrInvalidParam); ok {
				s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
				return
			}
			s.respondWithJSON(w, http.StatusInternalServerError, errorRs{err.Error()})
			return
		}
		consumers = make(map[string]map[string][]int32)
		if len(groupConsumers) != 0 {
			consumers[group] = groupConsumers
		}
	}

	encodedRes, err := json.MarshalIndent(consumers, "", "  ")
	if err != nil {
		s.actDesc.Log().WithError(err).Errorf("Failed to send HTTP response: status=%d, body=%v", http.StatusOK, encodedRes)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	encodedRes = prettyfmt.CollapseJSON(encodedRes)

	w.Header().Add(hdrContentType, "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(encodedRes); err != nil {
		s.actDesc.Log().WithError(err).Errorf("Failed to send HTTP response: status=%d, body=%v", http.StatusOK, encodedRes)
	}
}

// handleListTopics is an HTTP request handler for `GET /topics`
func (s *T) handleListTopics(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var err error

	pxy, err := s.getProxy(r)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	err = r.ParseForm()
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	_, withConfig := r.Form[prmTopicsWithConfig]
	_, withPartitions := r.Form[prmTopicsWithPartitions]

	topicsMetadata, err := pxy.ListTopics(withPartitions, withConfig)
	if err != nil {
		s.respondWithJSON(w, http.StatusInternalServerError, errorRs{err.Error()})
		return
	}

	if withPartitions || withConfig {
		topicMetadataViews := make(map[string]*topicMetadata)
		for _, tm := range topicsMetadata {
			topicMetadataView := newTopicMetadataView(withPartitions, withConfig, tm)
			topicMetadataViews[tm.Topic] = &topicMetadataView
		}
		s.respondWithJSON(w, http.StatusOK, topicMetadataViews)
		return
	}

	topics := make([]string, 0, len(topicsMetadata))
	for _, tm := range topicsMetadata {
		topics = append(topics, tm.Topic)
	}
	s.respondWithJSON(w, http.StatusOK, topics)
}

// handleGetTopicMetadata is an HTTP request handler for `GET /topics/{topic}`
func (s *T) handleGetTopicMetadata(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var err error

	pxy, err := s.getProxy(r)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	topic := mux.Vars(r)[prmTopic]

	err = r.ParseForm()
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	withConfig := true
	_, withPartitions := r.Form[prmTopicsWithPartitions]

	tm, err := pxy.GetTopicMetadata(topic, withPartitions, withConfig)
	if err != nil {
		s.respondWithJSON(w, http.StatusInternalServerError, errorRs{err.Error()})
		return
	}

	tm_view := newTopicMetadataView(withPartitions, withConfig, tm)
	s.respondWithJSON(w, http.StatusOK, tm_view)
}

func (s *T) handlePing(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("pong"))
}

// getHeapProfile responds with a pprof-formatted heap profile.
func getHeapProfile(w http.ResponseWriter, r *http.Request) {
	// Ensure up-to-date data.
	runtime.GC()
	w.Header().Set("Content-Type", "application/octet-stream")
	if err := pprof.Lookup("heap").WriteTo(w, 0); err != nil {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Could not get heap profile: %s\n", err)
	}
}

type produceRs struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

type consumeHeader struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type consumeRs struct {
	Key       []byte          `json:"key"`
	Value     []byte          `json:"value"`
	Partition int32           `json:"partition"`
	Offset    int64           `json:"offset"`
	Headers   []consumeHeader `json:"headers"`
}

type partitionInfo struct {
	Partition  int32  `json:"partition"`
	Begin      int64  `json:"begin"`
	End        int64  `json:"end"`
	Count      int64  `json:"count"`
	Offset     int64  `json:"offset"`
	Lag        int64  `json:"lag"`
	Metadata   string `json:"metadata,omitempty"`
	SparseAcks string `json:"sparse_acks,omitempty"`
}

type errorRs struct {
	Error string `json:"error"`
}

type topicConfig struct {
	Version int32             `json:"version"`
	Config  map[string]string `json:"config"`
}

type partitionMetadata struct {
	ID       int32   `json:"partition"`
	Leader   int32   `json:"leader"`
	Replicas []int32 `json:"replicas"`
	ISR      []int32 `json:"isr"`
}

type topicMetadata struct {
	Config     *topicConfig        `json:"config,omitempty"`
	Partitions []partitionMetadata `json:"partitions,omitempty"`
}

// queryParamBytes returns a URL query parameter as a slice of bytes. Unlike
// `http.FormValue` it never parses the request body, and it distinguishes an
// empty value (`[]byte{}`) from a missing one (`nil`).
func queryParamBytes(values url.Values, name string) []byte {
	v, ok := values[name]
	if !ok || len(v) == 0 {
		return nil
	}
	return []byte(v[0])
}

// respondWithJSON marshals `body` to a JSON string and sends it s an HTTP
// response body along with the specified `status` code.
func (s *T) respondWithJSON(w http.ResponseWriter, status int, body interface{}) {
	encodedRes, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		s.actDesc.Log().WithError(err).Errorf("Failed to send HTTP response: status=%d, body=%v", status, body)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Add(hdrContentType, "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(encodedRes); err != nil {
		s.actDesc.Log().WithError(err).Errorf("Failed to send HTTP response: status=%d, body=%v", status, body)
	}
}

func getGroupParam(r *http.Request, opt bool) (string, error) {
	r.ParseForm()
	groups := r.Form[prmGroup]
	if len(groups) > 1 || (!opt && len(groups) == 0) {
		return "", errors.Errorf("one consumer group is expected, but %d provided", len(groups))
	}
	if len(groups) == 0 {
		return "", nil
	}
	return groups[0], nil
}

// toEncoderPreservingNil converts a slice of bytes to `sarama.Encoder` but
// returns `nil` if the passed slice is `nil`.
func toEncoderPreservingNil(b []byte) sarama.Encoder {
	if b != nil {
		return sarama.StringEncoder(b)
	}
	return nil
}

func parseAck(r *http.Request, isConsReq bool) (proxy.Ack, error) {
	var partitionPrmName, offsetPrmName string
	if isConsReq {
		partitionPrmName = prmAckPartition
		offsetPrmName = prmAckOffset
	} else {
		partitionPrmName = prmPartition
		offsetPrmName = prmOffset
	}

	r.ParseForm()
	_, noAck := r.Form[prmNoAck]
	if noAck {
		return proxy.NoAck(), nil
	}
	var err error
	var partition int64
	partitionStr, partitionOk := r.Form[partitionPrmName]
	if partitionOk {
		partition, err = strconv.ParseInt(partitionStr[0], 10, 32)
		if err != nil || partition < 0 {
			return proxy.NoAck(), errors.Wrapf(err, "bad %s: %s", partitionPrmName, partitionStr)
		}
	}
	var offset int64
	offsetStr, offsetOk := r.Form[offsetPrmName]
	if offsetOk {
		offset, err = strconv.ParseInt(offsetStr[0], 10, 64)
		if err != nil || offset < 0 {
			return proxy.NoAck(), errors.Wrapf(err, "bad %s: %s", offsetPrmName, offsetStr)
		}
	}
	if partitionOk && offsetOk {
		return proxy.NewAck(int32(partition), offset)
	}
	if !partitionOk && !offsetOk {
		return proxy.AutoAck(), nil
	}
	return proxy.NoAck(), errors.Errorf("%s and %s either both should be provided or neither", partitionPrmName, offsetPrmName)
}

func newTopicMetadataView(withPartitions, withConfig bool, tm admin.TopicMetadata) topicMetadata {
	topicMetadataView := topicMetadata{}
	if withPartitions {
		for _, p := range tm.Partitions {
			partitionView := partitionMetadata{
				ID:       p.ID,
				Leader:   p.Leader,
				Replicas: p.Replicas,
				ISR:      p.ISR,
			}
			topicMetadataView.Partitions = append(topicMetadataView.Partitions, partitionView)
		}
	}
	if withConfig {
		topicConfig := topicConfig{
			Version: tm.Config.Version,
			Config:  tm.Config.Config,
		}
		topicMetadataView.Config = &topicConfig
	}
	return topicMetadataView
}
