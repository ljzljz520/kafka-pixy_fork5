// Package budget implements byte-weighted admission control for produce
// requests.
//
// It provides a two-level accounting model: a process-global limiter that
// covers produce traffic to all Kafka clusters, together with an independent
// per-cluster limiter, so that a single unavailable cluster cannot consume
// the admission quota of the others.
//
// The accounted weight is the payload size retained on behalf of a request
// (key, value, decoded Kafka headers, record framing and a duplication
// margin) multiplied by a duplication factor, it is NOT a precise bound on
// the Go process heap: sarama, Go runtime buffers, compression and transport
// layers allocate additional, only partially predictable memory on top of
// the accounted payload.
package budget

import (
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

// ErrQuotaExhausted is returned when neither the global nor the cluster
// limiter can accept the requested weight at admission time. It is a
// non-retriable-here overload signal; callers are expected to surface it as
// HTTP 429 / gRPC ResourceExhausted, clients may retry later.
var ErrQuotaExhausted = errors.New("produce byte budget exhausted")

// Limiter is a non-blocking byte-weighted counting limiter.
type Limiter struct {
	mu    sync.Mutex
	used  int64
	limit int64
}

// NewLimiter creates a limiter with the specified byte limit.
func NewLimiter(limit int64) *Limiter {
	return &Limiter{limit: limit}
}

// tryAcquire reserves n bytes. It returns false if the reservation would
// overflow the limit (including requests that alone exceed it).
func (l *Limiter) tryAcquire(n int64) bool {
	l.mu.Lock()
	if l.used+n > l.limit {
		l.mu.Unlock()
		return false
	}
	l.used += n
	l.mu.Unlock()
	return true
}

// release returns n bytes to the limiter. It is safe to underflow only when
// paired with a matching successful tryAcquire.
func (l *Limiter) release(n int64) {
	l.mu.Lock()
	l.used -= n
	l.mu.Unlock()
}

// Used reports the currently reserved number of bytes.
func (l *Limiter) Used() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.used
}

// Limit reports the limiter capacity in bytes.
func (l *Limiter) Limit() int64 {
	return l.limit
}

// Reservation represents bytes reserved in both the global and a cluster
// limiter for a single produce request. Ownership is transferred along the
// produce pipeline (request handler -> proxy -> producer) and must be
// released exactly once, on success, terminal failure, or shutdown.
type Reservation struct {
	global   *Limiter
	cluster  *Limiter
	size     int64
	released int32
}

// Size reports the reserved weight in bytes.
func (r *Reservation) Size() int64 {
	return r.size
}

// Release returns the reservation to both limiters. It is idempotent so that
// defensive callers can safely defer it.
func (r *Reservation) Release() {
	if !atomic.CompareAndSwapInt32(&r.released, 0, 1) {
		return
	}
	r.cluster.release(r.size)
	r.global.release(r.size)
}

// Shrink lowers the reservation to newSize, returning the difference to both
// limiters. It is used once the actual payload size becomes known and turns
// out smaller than the declared one (e.g. chunked HTTP uploads or form
// bodies). Growing a reservation is not supported.
func (r *Reservation) Shrink(newSize int64) {
	if newSize >= r.size {
		return
	}
	delta := r.size - newSize
	r.size = newSize
	r.cluster.release(delta)
	r.global.release(delta)
}

// Lane pairs the process-global limiter with one cluster's limiter and
// applies the configured duplication factor when accounting payloads.
type Lane struct {
	name    string
	global  *Limiter
	cluster *Limiter
	factor  int64
}

// Weight converts a payload byte count into the accounted weight, including
// the duplication margin.
func (l Lane) Weight(payloadBytes int64) int64 {
	return payloadBytes * l.factor
}

// Factor returns the duplication factor of the lane.
func (l Lane) Factor() int64 {
	return l.factor
}

// Name returns the cluster name associated with the lane.
func (l Lane) Name() string {
	return l.name
}

// Used reports bytes currently reserved on this cluster's limiter.
func (l Lane) Used() int64 {
	return l.cluster.Used()
}

// TryAcquire reserves weight in both limiters following the fixed
// global->cluster order. It returns ErrQuotaExhausted without blocking when
// there is not enough room.
func (l Lane) TryAcquire(weight int64) (*Reservation, error) {
	if !l.global.tryAcquire(weight) {
		return nil, ErrQuotaExhausted
	}
	if !l.cluster.tryAcquire(weight) {
		l.global.release(weight)
		return nil, ErrQuotaExhausted
	}
	return &Reservation{global: l.global, cluster: l.cluster, size: weight}, nil
}

// Config configures a Manager.
type Config struct {
	// GlobalLimit is the byte budget shared by all clusters in the process.
	GlobalLimit int64
	// DefaultClusterLimit is the per-cluster byte budget used when a cluster
	// does not provide its own override.
	DefaultClusterLimit int64
	// ClusterLimits holds per-cluster byte budget overrides.
	ClusterLimits map[string]int64
	// DuplicationFactor is the multiplier applied to every accounted payload
	// to cover the copies of it held concurrently by the request handler,
	// dispatcher channels and sarama in-flight/request encoding.
	DuplicationFactor int64
	// MaxConcurrentRequests bounds how many requests may be parsed/staged
	// before having acquired a formal byte reservation.
	MaxConcurrentRequests int
}

// Manager owns the global and per-cluster limiters and the staging gate.
type Manager struct {
	global *Limiter
	lanes  map[string]Lane
	cfg    Config
	gate   chan struct{}
}

// NewManager creates a Manager from the provided config.
func NewManager(cfg Config) *Manager {
	m := &Manager{
		global: NewLimiter(cfg.GlobalLimit),
		lanes:  make(map[string]Lane),
		cfg:    cfg,
		gate:   make(chan struct{}, cfg.MaxConcurrentRequests),
	}
	return m
}

// Lane returns the admission lane for the named cluster, creating it lazily
// with the cluster-specific limit or the default one.
func (m *Manager) Lane(name string) Lane {
	if l, ok := m.lanes[name]; ok {
		return l
	}
	limit := m.cfg.DefaultClusterLimit
	if l, ok := m.cfg.ClusterLimits[name]; ok {
		limit = l
	}
	l := Lane{
		name:    name,
		global:  m.global,
		cluster: NewLimiter(limit),
		factor:  m.cfg.DuplicationFactor,
	}
	m.lanes[name] = l
	return l
}

// GlobalUsed reports the total number of bytes currently reserved across all
// clusters.
func (m *Manager) GlobalUsed() int64 {
	return m.global.Used()
}

// TryAcquireSlot tries to take one staging slot. It returns false when the
// number of requests concurrently being parsed/admitted is at the cap.
func (m *Manager) TryAcquireSlot() bool {
	select {
	case m.gate <- struct{}{}:
		return true
	default:
		return false
	}
}

// ReleaseSlot returns a staging slot; calls must pair successful
// TryAcquireSlot calls.
func (m *Manager) ReleaseSlot() {
	<-m.gate
}
