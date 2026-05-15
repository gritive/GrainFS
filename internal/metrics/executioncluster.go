package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// ExecutionClusterQueueDepth tracks queued cluster execution jobs.
	ExecutionClusterQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_execution_cluster_queue_depth",
		Help: "Current number of queued cluster execution jobs.",
	})

	// ExecutionClusterJobDuration measures cluster execution job duration.
	ExecutionClusterJobDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "grainfs_execution_cluster_job_duration_seconds",
		Help:    "Cluster execution job duration in seconds.",
		Buckets: prometheus.DefBuckets,
	})

	// ExecutionClusterRetriesTotal counts cluster execution backend retries.
	ExecutionClusterRetriesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_execution_cluster_retries_total",
		Help: "Total cluster execution backend retries.",
	})

	// ExecutionClusterTimeoutsTotal counts timed out cluster execution jobs.
	ExecutionClusterTimeoutsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_execution_cluster_timeouts_total",
		Help: "Total timed out cluster execution jobs.",
	})

	// ExecutionClusterWorkerFailuresTotal counts failed cluster execution jobs.
	ExecutionClusterWorkerFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_execution_cluster_worker_failures_total",
		Help: "Total failed cluster execution jobs after worker execution.",
	})

	// ExecutionClusterAggregationFailuresTotal counts aggregation failures.
	ExecutionClusterAggregationFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_execution_cluster_aggregation_failures_total",
		Help: "Total cluster execution aggregation failures.",
	})
)
