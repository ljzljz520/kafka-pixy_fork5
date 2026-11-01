package proxy

import (
	"github.com/mailgun/kafka-pixy/budget"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/pkg/errors"
)

// Set represents a collection of proxy.T instances with a default value.
type Set struct {
	proxies    map[string]*T
	defaultPxy *T
	budget     *budget.Manager
}

// NewSet creates a proxy.Set from a cluster-to-proxy map and a default
// proxy. It also wires the shared produce byte budget: one process-global
// limiter covering all clusters, an independent limiter per cluster (with an
// optional per-cluster override), and the staging gate bounding requests
// that have not acquired a formal byte reservation yet.
func NewSet(appCfg *config.App, proxies map[string]*T, defaultPxy *T) *Set {
	if len(proxies) < 1 {
		panic("set must contain at least one proxy")
	}
	if defaultPxy == nil {
		panic("default proxy must be provided")
	}
	if appCfg == nil {
		appCfg = config.DefaultApp("default")
	}
	bcfg := appCfg.ProduceBudgetNormalized()
	clusterLimits := make(map[string]int64)
	for name, pxyCfg := range appCfg.Proxies {
		if _, ok := proxies[name]; !ok {
			continue
		}
		if pxyCfg != nil && pxyCfg.Producer.BudgetBytes > 0 {
			clusterLimits[name] = pxyCfg.Producer.BudgetBytes
		}
	}
	mgr := budget.NewManager(budget.Config{
		GlobalLimit:           bcfg.TotalBytes,
		DefaultClusterLimit:   bcfg.PerClusterBytes,
		ClusterLimits:         clusterLimits,
		DuplicationFactor:     int64(bcfg.DuplicationFactor),
		MaxConcurrentRequests: bcfg.MaxConcurrentRequests,
	})
	for name, pxy := range proxies {
		pxy.setAdmission(mgr.Lane(name))
	}
	return &Set{proxies: proxies, defaultPxy: defaultPxy, budget: mgr}
}

// Get returns a proxy for a cluster name. If there is no proxy configured for
// the cluster name, then the default proxy is returned.
func (s *Set) Get(cluster string) (*T, error) {
	if cluster == "" {
		return s.defaultPxy, nil
	}
	if pxy := s.proxies[cluster]; pxy != nil {
		return pxy, nil
	}
	return nil, errors.Errorf("proxy `%s` does not exist", cluster)
}

// TryAcquireProduceSlot takes a staging slot bounding the number of produce
// requests that can be concurrently parsed before acquiring a formal byte
// reservation. It returns false when the cap is reached; callers must respond
// with HTTP 429 / gRPC ResourceExhausted.
func (s *Set) TryAcquireProduceSlot() bool {
	return s.budget.TryAcquireSlot()
}

// ReleaseProduceSlot returns a staging slot acquired with
// TryAcquireProduceSlot.
func (s *Set) ReleaseProduceSlot() {
	s.budget.ReleaseSlot()
}
