package config

import (
	"strings"
	"testing"
)

func TestProduceBudgetNormalized(t *testing.T) {
	app := &App{}
	b := app.ProduceBudgetNormalized()
	if b.TotalBytes != defaultProduceBudgetTotal {
		t.Fatalf("total=%d, want %d", b.TotalBytes, defaultProduceBudgetTotal)
	}
	if b.PerClusterBytes != defaultProduceBudgetPerCluster {
		t.Fatalf("per_cluster=%d, want %d", b.PerClusterBytes, defaultProduceBudgetPerCluster)
	}
	if b.DuplicationFactor != defaultProduceBudgetDupFactor {
		t.Fatalf("factor=%d, want %d", b.DuplicationFactor, defaultProduceBudgetDupFactor)
	}
	if b.MaxConcurrentRequests != defaultProduceBudgetMaxRequests {
		t.Fatalf("max_requests=%d, want %d", b.MaxConcurrentRequests, defaultProduceBudgetMaxRequests)
	}
	// Explicit values must be preserved.
	app.ProduceBudget = ProduceBudgetCfg{
		TotalBytes:            1,
		PerClusterBytes:       2,
		DuplicationFactor:     3,
		MaxConcurrentRequests: 4,
	}
	b = app.ProduceBudgetNormalized()
	if b != app.ProduceBudget {
		t.Fatalf("explicit budget changed: %+v", b)
	}
}

func TestProduceBudgetValidation(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "per cluster above total",
			yaml: "" +
				"proxies:\n" +
				"  default: {}\n" +
				"produce_budget:\n" +
				"  total_bytes: 1000\n" +
				"  per_cluster_bytes: 2000\n",
			want: "per_cluster_bytes must be <=",
		},
		{
			name: "factor out of range",
			yaml: "" +
				"proxies:\n" +
				"  default: {}\n" +
				"produce_budget:\n" +
				"  duplication_factor: 9\n",
			want: "duplication_factor must be in [1, 8]",
		},
		{
			name: "cluster budget below one max-sized record",
			yaml: "" +
				"proxies:\n" +
				"  default:\n" +
				"    producer:\n" +
				"      budget_bytes: 100\n" +
				"produce_budget:\n" +
				"  duplication_factor: 1\n",
			want: "must be >= producer.max_message_bytes*duplication_factor",
		},
		{
			name: "cluster budget above total",
			yaml: "" +
				"proxies:\n" +
				"  default:\n" +
				"    producer:\n" +
				"      budget_bytes: 5000000\n" +
				"produce_budget:\n" +
				"  total_bytes: 4000000\n" +
				"  per_cluster_bytes: 2000000\n" +
				"  duplication_factor: 1\n",
			want: "must be <= produce_budget.total_bytes",
		},
	}
	for _, tc := range cases {
		if _, err := FromYAML([]byte(tc.yaml)); err == nil ||
			!strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: want error containing %q, got %v", tc.name, tc.want, err)
		}
	}
}

func TestProduceBudgetValidOverride(t *testing.T) {
	data := []byte("" +
		"proxies:\n" +
		"  default:\n" +
		"    producer:\n" +
		"      budget_bytes: 3000000\n" +
		"produce_budget:\n" +
		"  total_bytes: 4000000\n" +
		"  per_cluster_bytes: 2000000\n" +
		"  duplication_factor: 1\n" +
		"  max_concurrent_requests: 7\n")
	app, err := FromYAML(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := app.Proxies["default"].Producer.BudgetBytes; got != 3000000 {
		t.Fatalf("budget_bytes=%d, want 3000000", got)
	}
	if got := app.ClusterBudgetBytes("default", app.Proxies["default"]); got != 3000000 {
		t.Fatalf("ClusterBudgetBytes=%d, want 3000000", got)
	}
	if app.ProduceBudget.MaxConcurrentRequests != 7 {
		t.Fatalf("max_concurrent_requests not parsed: %+v", app.ProduceBudget)
	}
}
