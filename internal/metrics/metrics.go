package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HTTPRequestsTotal counts HTTP requests by method and status.
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_http_requests_total",
		Help: "Total number of HTTP requests.",
	}, []string{"method", "status"})

	// HTTPRequestDuration measures HTTP request duration by method.
	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "grainfs_http_request_duration_seconds",
		Help:    "HTTP request duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method"})

	// ECEncodeDuration measures erasure coding encode duration.
	ECEncodeDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "grainfs_ec_encode_duration_seconds",
		Help:    "Erasure coding encode duration in seconds.",
		Buckets: prometheus.DefBuckets,
	})

	// ECDecodeDuration measures erasure coding decode duration.
	ECDecodeDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "grainfs_ec_decode_duration_seconds",
		Help:    "Erasure coding decode duration in seconds.",
		Buckets: prometheus.DefBuckets,
	})

	// StorageBytesTotal tracks total bytes stored.
	StorageBytesTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_storage_bytes_total",
		Help: "Total bytes stored.",
	})

	// BucketsTotal tracks total number of buckets.
	BucketsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_buckets_total",
		Help: "Total number of buckets.",
	})

	// ObjectsTotal tracks total number of objects.
	ObjectsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_objects_total",
		Help: "Total number of objects.",
	})

	// CacheInvalidationTotal counts cache invalidation operations.
	CacheInvalidationTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_cache_invalidation_total",
		Help: "Total number of cache invalidation operations.",
	}, []string{"bucket", "protocol"}) // protocol: vfs, nfs, cached_backend

	// CacheInvalidationDuration measures cache invalidation operation duration.
	CacheInvalidationDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "grainfs_cache_invalidation_duration_seconds",
		Help:    "Cache invalidation operation duration in seconds.",
		Buckets: prometheus.DefBuckets,
	})

	// CacheStatHits counts cache hits in stat cache.
	CacheStatHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_cache_stat_hits_total",
		Help: "Total number of cache hits in stat cache.",
	})

	// CacheStatMisses counts cache misses in stat cache.
	CacheStatMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_cache_stat_misses_total",
		Help: "Total number of cache misses in stat cache.",
	})

	// DeletedMarkersTotal tracks current number of deleted file markers.
	DeletedMarkersTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_deleted_markers_total",
		Help: "Current number of deleted file markers in memory.",
	})

	// RegistrySize tracks current number of registered cache invalidators.
	RegistrySize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_cache_registry_size",
		Help: "Current number of registered cache invalidators.",
	})

	// NFSv4BufferPoolGets tracks total buffer pool get operations by buffer size.
	NFSv4BufferPoolGets = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_nfsv4_buffer_pool_gets_total",
		Help: "Total number of buffer pool get operations.",
	}, []string{"size"})

	// NFSv4BufferPoolMisses tracks buffer pool misses (fallback allocations).
	NFSv4BufferPoolMisses = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_nfsv4_buffer_pool_misses_total",
		Help: "Total number of buffer pool misses (temporary allocations).",
	}, []string{"size"})

	// NFSv4BufferSizeInUse tracks current buffer size in use by pool type.
	NFSv4BufferSizeInUse = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_nfsv4_buffer_size_bytes",
		Help: "Current buffer size in use by pool type.",
	}, []string{"size"})

	// SplitBrainSuspected is 1 when split brain is detected in the cluster, 0 otherwise.
	SplitBrainSuspected = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_split_brain_suspected",
		Help: "1 if split brain is suspected (multiple leaders or large term divergence), 0 otherwise.",
	})

	// ScrubShardErrorsTotal counts shard errors (missing + corrupt) detected during scrubbing.
	ScrubShardErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_shard_errors_total",
		Help: "Total shard errors detected during scrubbing.",
	})

	// ScrubRepairedTotal counts objects successfully repaired by the scrubber.
	ScrubRepairedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_repaired_total",
		Help: "Total objects repaired by the scrubber.",
	})

	// ECDegradedTotal counts EC objects that could not be repaired (too many shards lost).
	ECDegradedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_ec_degraded_total",
		Help: "Total EC objects that could not be repaired.",
	})

	// ScrubObjectsCheckedTotal counts objects checked by the scrubber.
	ScrubObjectsCheckedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_objects_checked_total",
		Help: "Total objects checked by the scrubber.",
	})

	// ScrubSkippedOverCapTotal counts objects skipped because repair cap was reached.
	ScrubSkippedOverCapTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_skipped_over_cap_total",
		Help: "Total objects skipped because max_repairs_per_cycle was reached.",
	})

	// ScrubPlainMigratedTotal counts plain objects re-encoded to EC by the scrubber.
	ScrubPlainMigratedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_plain_migrated_total",
		Help: "Total plain objects re-encoded to EC by the scrubber.",
	})

	// ScrubPlainMigrateErrorTotal counts plain→EC migration errors.
	ScrubPlainMigrateErrorTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_plain_migrate_error_total",
		Help: "Total errors during plain→EC migration by the scrubber.",
	})

	// ScrubMigrationSkippedOverCapTotal counts plain objects skipped because migration cap was reached.
	ScrubMigrationSkippedOverCapTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_migration_skipped_over_cap_total",
		Help: "Total plain objects skipped because max_migrations_per_cycle was reached.",
	})
)
