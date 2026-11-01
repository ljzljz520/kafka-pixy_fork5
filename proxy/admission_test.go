package proxy

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/budget"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/producer"
)

func newAdmissionTestProxy(t *testing.T, bcfg budget.Config) (*T, *budget.Manager) {
	cfg := config.DefaultProxy()
	cfg.Producer.MaxMessageBytes = 1000
	cfg.Kafka.Version.Set(sarama.V0_11_0_0)
	mgr := budget.NewManager(bcfg)
	p := &T{cfg: cfg, producer: &producer.T{}}
	p.setAdmission(mgr.Lane("default"))
	return p, mgr
}

func testBudgetCfg(global, cluster int64) budget.Config {
	return budget.Config{
		GlobalLimit:           global,
		DefaultClusterLimit:   cluster,
		ClusterLimits:         map[string]int64{},
		DuplicationFactor:     2,
		MaxConcurrentRequests: 4,
	}
}

// Billing covers key, value, headers and record framing, all multiplied by
// the duplication factor; CommitValue returns over-reserved bytes.
func TestAdmitProduceBillingAndCommit(t *testing.T) {
	p, mgr := newAdmissionTestProxy(t, testBudgetCfg(4096, 4096))
	headers := []sarama.RecordHeader{{Key: []byte("h"), Value: []byte("hv")}}

	a, err := p.AdmitProduce(PayloadSpec{
		KeyLength:   3,
		ValueLength: 100,
		Headers:     headers,
	})
	if err != nil {
		t.Fatalf("unexpected admission error: %v", err)
	}
	// overhead = 26 + 4 per header = 30; payload = 30 + 3 + 3 + 100 = 136;
	// weight with factor 2 is 272.
	if used := mgr.GlobalUsed(); used != 272 {
		t.Fatalf("global used=%d, want 272", used)
	}
	// Max value: 1000 - 30 - 3 - 3 = 964.
	if got := a.MaxValueLength(); got != 964 {
		t.Fatalf("MaxValueLength=%d, want 964", got)
	}
	// Committing a smaller value shrinks the reservation:
	// fixed weight 2*(30+3+3)=72 plus 2*50=100 => 172.
	if err := a.CommitValue(50); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if used := mgr.GlobalUsed(); used != 172 {
		t.Fatalf("global used=%d after commit, want 172", used)
	}
	a.Release()
	if used := mgr.GlobalUsed(); used != 0 {
		t.Fatalf("global used=%d after release, want 0", used)
	}
}

// An unknown declared length (chunked upload) reserves the worst case and is
// shrunk once the actual size is known.
func TestAdmitProduceUnknownLengthReservesWorstCase(t *testing.T) {
	p, mgr := newAdmissionTestProxy(t, testBudgetCfg(4096, 4096))

	a, err := p.AdmitProduce(PayloadSpec{KeyLength: 3, ValueLength: -1})
	if err != nil {
		t.Fatalf("unexpected admission error: %v", err)
	}
	// payload = 26 + 3 + 971 = 1000, weight 2000.
	if used := mgr.GlobalUsed(); used != 2000 {
		t.Fatalf("global used=%d, want 2000", used)
	}
	if err := a.CommitValue(100); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// fixed weight 2*(26+3)=58 plus 2*100 => 258.
	if used := mgr.GlobalUsed(); used != 258 {
		t.Fatalf("global used=%d after commit, want 258", used)
	}
	a.Release()
}

// Oversize values are rejected at admission, before reading the body.
func TestAdmitProduceOversize(t *testing.T) {
	p, mgr := newAdmissionTestProxy(t, testBudgetCfg(4096, 4096))

	if _, err := p.AdmitProduce(PayloadSpec{KeyLength: 3, ValueLength: 972}); err != ErrMessageTooLarge {
		t.Fatalf("want ErrMessageTooLarge, got %v", err)
	}
	if used := mgr.GlobalUsed(); used != 0 {
		t.Fatalf("rejected admission must not reserve bytes, used=%d", used)
	}
}

// An oversize actual value discovered after the read releases the reservation.
func TestAdmitProduceCommitOversizeReleases(t *testing.T) {
	p, mgr := newAdmissionTestProxy(t, testBudgetCfg(4096, 4096))

	a, err := p.AdmitProduce(PayloadSpec{KeyLength: 3, ValueLength: -1})
	if err != nil {
		t.Fatalf("unexpected admission error: %v", err)
	}
	if err := a.CommitValue(a.MaxValueLength() + 1); err != ErrMessageTooLarge {
		t.Fatalf("want ErrMessageTooLarge, got %v", err)
	}
	if used := mgr.GlobalUsed(); used != 0 {
		t.Fatalf("oversize commit must release, used=%d", used)
	}
	// Release after an oversize commit is harmless (idempotent token).
	a.Release()
	if used := mgr.GlobalUsed(); used != 0 {
		t.Fatalf("double release changed accounting: used=%d", used)
	}
}

// Headers are rejected before reservation on Kafka versions older than 0.11.
func TestAdmitProduceHeadersUnsupported(t *testing.T) {
	cfg := config.DefaultProxy()
	cfg.Producer.MaxMessageBytes = 1000
	mgr := budget.NewManager(testBudgetCfg(4096, 4096))
	p := &T{cfg: cfg, producer: &producer.T{}}
	p.setAdmission(mgr.Lane("default"))

	_, err := p.AdmitProduce(PayloadSpec{
		ValueLength: 10,
		Headers:     []sarama.RecordHeader{{Key: []byte("h"), Value: []byte("v")}},
	})
	if err != ErrHeadersUnsupported {
		t.Fatalf("want ErrHeadersUnsupported, got %v", err)
	}
	if used := mgr.GlobalUsed(); used != 0 {
		t.Fatalf("rejected admission must not reserve bytes, used=%d", used)
	}
}

// A stopped producer cannot admit anything.
func TestAdmitProduceUnavailable(t *testing.T) {
	p, mgr := newAdmissionTestProxy(t, testBudgetCfg(4096, 4096))
	p.producer = nil
	if _, err := p.AdmitProduce(PayloadSpec{ValueLength: 10}); err != ErrUnavailable {
		t.Fatalf("want ErrUnavailable, got %v", err)
	}
	if used := mgr.GlobalUsed(); used != 0 {
		t.Fatalf("unavailable admission must not reserve bytes, used=%d", used)
	}
}

// Missing cluster quota is rolled back from the global limiter and reported
// as ErrQuotaExhausted; other clusters keep their independent quota.
func TestAdmitProduceQuotaExhausted(t *testing.T) {
	mgr := budget.NewManager(budget.Config{
		GlobalLimit:           4096,
		DefaultClusterLimit:   50, // below the minimum accounted weight 52
		ClusterLimits:         map[string]int64{},
		DuplicationFactor:     2,
		MaxConcurrentRequests: 4,
	})
	cfg := config.DefaultProxy()
	cfg.Producer.MaxMessageBytes = 1000
	p := &T{cfg: cfg, producer: &producer.T{}}
	p.setAdmission(mgr.Lane("default"))

	if _, err := p.AdmitProduce(PayloadSpec{ValueLength: 1}); err != ErrQuotaExhausted {
		t.Fatalf("want ErrQuotaExhausted, got %v", err)
	}
	if used := mgr.GlobalUsed(); used != 0 {
		t.Fatalf("failed cluster acquisition must roll back global, used=%d", used)
	}
}

// submit releases the admission and reports ErrUnavailable when the producer
// is stopped between admission and enqueue.
func TestSubmitAfterStopReleasesAdmission(t *testing.T) {
	p, mgr := newAdmissionTestProxy(t, testBudgetCfg(4096, 4096))
	a, err := p.AdmitProduce(PayloadSpec{ValueLength: 10})
	if err != nil {
		t.Fatalf("unexpected admission error: %v", err)
	}
	p.producer = nil
	if err := p.AsyncProduce(a, "test.4", nil, sarama.StringEncoder("v"), nil); err != ErrUnavailable {
		t.Fatalf("want ErrUnavailable, got %v", err)
	}
	if used := mgr.GlobalUsed(); used != 0 {
		t.Fatalf("rejected submit must release reservation, used=%d", used)
	}
}
