package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// shardPackStore writer-actor metrics. See
// docs/superpowers/specs/2026-05-24-shardpack-writer-actor-design.md § Metrics.
var (
	// ShardPackBatchSize records how many shard-write records the actor
	// commits per BadgerDB-equivalent transaction (per fsync). Skewed toward
	// 1 under low load; higher buckets under concurrency confirm batching.
	ShardPackBatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "grainfs_shardpack_batch_size",
		Help:    "Shard-pack records applied per actor commit batch.",
		Buckets: []float64{1, 2, 4, 8, 16, 32, 64},
	})

	// ShardPackWALFlushSeconds measures fsync wait time on the data WAL,
	// observed once per actor commit batch. Long-tail outliers indicate
	// fsync stalls (disk pressure, dirty-page backlog).
	ShardPackWALFlushSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "grainfs_shardpack_wal_flush_seconds",
		Help:    "Data-WAL fsync wait time per shard-pack actor commit batch.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 14),
	})

	// ShardPackBatchAbortsTotal counts batches that hit a mid-batch failure
	// (WAL append, WAL flush, blob write, or rotate). Zero in healthy
	// operation; rising values indicate disk/WAL stress that callers see
	// as retry-able errors.
	ShardPackBatchAbortsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_shardpack_batch_aborts_total",
		Help: "Shard-pack actor batches that aborted mid-way, by reason.",
	}, []string{"reason"})
)
