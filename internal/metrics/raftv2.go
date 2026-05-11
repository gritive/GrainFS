package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// proposeBuckets are the histogram buckets for raft v2 propose latency.
// Range: 1ms–5s covers expected p99 well under load.
var proposeBuckets = []float64{0.001, 0.005, 0.010, 0.050, 0.100, 0.500, 1.0, 5.0}

// waitAppliedBuckets are the histogram buckets for WaitApplied latency.
var waitAppliedBuckets = []float64{0.001, 0.010, 0.050, 0.100, 0.500, 1.0}

var (
	// RaftV2ProposeCount counts ProposeWait calls by outcome.
	// Labels: outcome ∈ {success, not_leader, timeout, error}.
	RaftV2ProposeCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_raft_v2_propose_total",
		Help: "Total ProposeWait calls through the v2 adapter, by outcome.",
	}, []string{"outcome"})

	// RaftV2ProposeLatency measures ProposeWait end-to-end latency by outcome.
	// Labels: outcome ∈ {success, not_leader, timeout, error}.
	RaftV2ProposeLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "grainfs_raft_v2_propose_latency_seconds",
		Help:    "ProposeWait latency in seconds through the v2 adapter, by outcome.",
		Buckets: proposeBuckets,
	}, []string{"outcome"})

	// RaftV2WaitAppliedLatency measures WaitApplied wall-clock duration.
	RaftV2WaitAppliedLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "grainfs_raft_v2_wait_applied_latency_seconds",
		Help:    "WaitApplied latency in seconds through the v2 adapter.",
		Buckets: waitAppliedBuckets,
	})

	// RaftV2BootstrapOutcome counts Bootstrap calls by outcome.
	// Labels: outcome ∈ {success, error}.
	RaftV2BootstrapOutcome = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_raft_v2_bootstrap_total",
		Help: "Total Bootstrap calls through the v2 adapter, by outcome.",
	}, []string{"outcome"})

	// RaftV2StopCount counts Close() calls on the v2 adapter.
	// Non-zero rates during steady-state soak indicate unexpected restarts.
	RaftV2StopCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_raft_v2_stop_total",
		Help: "Total Close() calls on the v2 adapter. Unexpected spikes indicate restarts during soak.",
	})
)
