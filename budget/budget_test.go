package budget

import (
	"sync"
	"testing"
)

func newTestManager() *Manager {
	return NewManager(Config{
		GlobalLimit:           100,
		DefaultClusterLimit:   60,
		ClusterLimits:         map[string]int64{"big": 80},
		DuplicationFactor:     2,
		MaxConcurrentRequests: 2,
	})
}

// Both global and per-cluster limits must allow the reservation.
func TestTryAcquireGlobalAndCluster(t *testing.T) {
	m := newTestManager()
	a := m.Lane("a")

	r, err := a.TryAcquire(a.Weight(30)) // weight 60: fits both
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.GlobalUsed() != 60 {
		t.Fatalf("global used=%d, want 60", m.GlobalUsed())
	}
	if a.Used() != 60 {
		t.Fatalf("cluster used=%d, want 60", a.Used())
	}

	// Same cluster cannot exceed its 60 bytes even though global has room.
	if _, err = a.TryAcquire(1); err != ErrQuotaExhausted {
		t.Fatalf("want ErrQuotaExhausted, got %v", err)
	}
	r.Release()
	if m.GlobalUsed() != 0 || a.Used() != 0 {
		t.Fatalf("expected counters released, got global=%d cluster=%d", m.GlobalUsed(), a.Used())
	}
}

// A stuck cluster cannot exhaust quota of other clusters thanks to the
// per-cluster limit, and the global limit bounds aggregate admission.
func TestClusterIsolation(t *testing.T) {
	m := newTestManager()
	a := m.Lane("a")
	b := m.Lane("b")

	ra, err := a.TryAcquire(60) // cluster a at its cap, global at 60
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Cluster b still has its own full quota, and global has 40 bytes left.
	rb, err := b.TryAcquire(40)
	if err != nil {
		t.Fatalf("cluster b must not be starved by cluster a: %v", err)
	}
	// Aggregate global limit is now reached.
	if _, err = b.TryAcquire(1); err != ErrQuotaExhausted {
		t.Fatalf("want ErrQuotaExhausted globally, got %v", err)
	}
	ra.Release()
	rb.Release()

	// Cluster-specific override is honored.
	big := m.Lane("big")
	if _, err = big.TryAcquire(80); err != nil {
		t.Fatalf("cluster override limit not honored: %v", err)
	}
}

// A single reservation larger than the limit is rejected.
func TestAcquireLargerThanLimit(t *testing.T) {
	m := newTestManager()
	if _, err := m.Lane("a").TryAcquire(61); err != ErrQuotaExhausted {
		t.Fatalf("want ErrQuotaExhausted, got %v", err)
	}
}

// Shrink returns the difference but never grows a reservation.
func TestShrink(t *testing.T) {
	m := newTestManager()
	a := m.Lane("a")
	r, err := a.TryAcquire(60)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.Shrink(40)
	if r.Size() != 40 || a.Used() != 40 || m.GlobalUsed() != 40 {
		t.Fatalf("shrink not accounted: size=%d cluster=%d global=%d", r.Size(), a.Used(), m.GlobalUsed())
	}
	// Growing is a no-op.
	r.Shrink(50)
	if r.Size() != 40 || a.Used() != 40 {
		t.Fatalf("shrink must not grow reservation")
	}
	r.Release()
	if a.Used() != 0 || m.GlobalUsed() != 0 {
		t.Fatalf("counters not released")
	}
}

// Release is idempotent.
func TestReleaseIdempotent(t *testing.T) {
	m := newTestManager()
	r, err := m.Lane("a").TryAcquire(10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.Release()
	r.Release()
	if m.GlobalUsed() != 0 {
		t.Fatalf("double release changed accounting: %d", m.GlobalUsed())
	}
}

// The staging gate bounds concurrent pre-admission work.
func TestStagingGate(t *testing.T) {
	m := newTestManager()
	if !m.TryAcquireSlot() || !m.TryAcquireSlot() {
		t.Fatal("expected to acquire two slots")
	}
	if m.TryAcquireSlot() {
		t.Fatal("third slot must be rejected while gate is full")
	}
	m.ReleaseSlot()
	if !m.TryAcquireSlot() {
		t.Fatal("slot should be available after release")
	}
	m.ReleaseSlot()
	m.ReleaseSlot()
}

func TestWeightIncludesFactor(t *testing.T) {
	m := newTestManager()
	l := m.Lane("a")
	if got := l.Weight(21); got != 42 {
		t.Fatalf("weight=%d, want 42", got)
	}
	if l.Factor() != 2 {
		t.Fatalf("factor=%d, want 2", l.Factor())
	}
}

// Repeated concurrent acquisitions/releases must keep the counters exact and
// never over the limits.
func TestConcurrentAcquireRelease(t *testing.T) {
	m := NewManager(Config{
		GlobalLimit:           100,
		DefaultClusterLimit:   100,
		ClusterLimits:         map[string]int64{},
		DuplicationFactor:     1,
		MaxConcurrentRequests: 64,
	})
	lane := m.Lane("default")
	const goroutines, iterations, weight = 50, 200, 10

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for acquired := 0; acquired < iterations; {
				r, err := lane.TryAcquire(weight)
				if err != nil {
					if used := m.GlobalUsed(); used > 100 {
						t.Errorf("global used %d over limit", used)
					}
					continue
				}
				if used := m.GlobalUsed(); used > 100 {
					t.Errorf("global used %d over limit", used)
				}
				r.Release()
				acquired++
			}
		}()
	}
	wg.Wait()
	if used := m.GlobalUsed(); used != 0 {
		t.Fatalf("global used=%d after all releases, want 0", used)
	}
	if used := lane.Used(); used != 0 {
		t.Fatalf("cluster used=%d after all releases, want 0", used)
	}
}
