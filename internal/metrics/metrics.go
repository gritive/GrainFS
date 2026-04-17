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
)
