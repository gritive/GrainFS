package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Lifecycle metrics. See ADR 0011 and the Lifecycle MinIO-Parity Phase 1 plan.
//
// LifecycleAbortedUploads carries a node_id label because the MPU worker is
// per-node — operators want to attribute aborts to the node that performed
// them in a multi-node cluster.
var (
	LifecycleAbortedUploads = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_lifecycle_aborted_uploads_total",
		Help: "Multipart uploads reclaimed via AbortIncompleteMultipartUpload, labelled by bucket and node_id.",
	}, []string{"bucket", "node_id"})

	LifecycleDeleteMarkersReclaimed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_lifecycle_delete_markers_reclaimed_total",
		Help: "Expired lone delete markers removed by ExpiredObjectDeleteMarker.",
	}, []string{"bucket"})

	LifecycleRuleMatch = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_lifecycle_rule_match_total",
		Help: "Lifecycle rule matches by action (expire, expire_noncurrent, expire_delete_marker, abort_mpu).",
	}, []string{"rule_id", "action"})

	LifecycleCycleSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "grainfs_lifecycle_cycle_seconds",
		Help:    "Lifecycle scan cycle duration per bucket.",
		Buckets: []float64{0.1, 1, 5, 30, 120, 600, 3600},
	}, []string{"bucket"})

	LifecycleGroupVersions = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "grainfs_lifecycle_group_versions",
		Help:    "Versions per ObjectKeyGroup emitted to the lifecycle worker.",
		Buckets: []float64{1, 10, 100, 1000, 10000, 100000},
	})
)
