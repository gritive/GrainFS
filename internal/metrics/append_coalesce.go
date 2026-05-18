package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Phase B3 AppendObject coalesce metrics. See design doc § Metrics.
var (
	// AppendCoalesceTotal counts coalesce attempts by result.
	// result label: "success" | "abort".
	AppendCoalesceTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_append_coalesce_total",
		Help: "Total append coalesce attempts by result.",
	}, []string{"result"})

	// AppendCoalesceBytes counts plaintext bytes coalesced into EC shards.
	AppendCoalesceBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_append_coalesce_bytes",
		Help: "Total plaintext bytes coalesced into EC shards.",
	})

	// AppendCoalesceLatencySeconds measures single-coalesce wall time.
	AppendCoalesceLatencySeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "grainfs_append_coalesce_latency_seconds",
		Help:    "Single coalesce operation latency in seconds.",
		Buckets: prometheus.DefBuckets,
	})

	// AppendForwardOnReadTotal counts forward-on-read fallbacks served by a
	// peer when the local owner blob was absent (Phase B1 + B2 fallback).
	AppendForwardOnReadTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_append_forward_on_read_total",
		Help: "Total append-segment / coalesced reads served via peer forward-on-read.",
	})
)
