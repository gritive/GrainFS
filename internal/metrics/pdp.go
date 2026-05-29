package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// PDPRequestsTotal counts External PDP consultations.
//   - decision: "allow" | "deny" | "error"
//   - error_type: "" for non-errors, else timeout|transport|status|decode|invalid_decision
//   - failure_policy: "closed" | "open" (policy in effect for the request)
var PDPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "grainfs_iam_pdp_requests_total",
	Help: "External PDP authorization consultations by decision, error_type, and failure_policy.",
}, []string{"decision", "error_type", "failure_policy"})

// PDPRequestDuration is the PDP round-trip latency in seconds.
var PDPRequestDuration = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "grainfs_iam_pdp_request_duration_seconds",
	Help:    "External PDP authorization round-trip latency.",
	Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
})

// PDPCacheTotal counts External PDP decision cache outcomes.
//   - result: "hit" | "miss" | "grace" (served stale within grace window)
//   - decision: "allow" | "deny"
var PDPCacheTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "grainfs_iam_pdp_cache_total",
	Help: "External PDP decision cache outcomes by result and decision.",
}, []string{"result", "decision"})

// PDPCacheEntries is the current External PDP decision cache entry count.
var PDPCacheEntries = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "grainfs_iam_pdp_cache_entries",
	Help: "Current External PDP decision cache entry count.",
})
