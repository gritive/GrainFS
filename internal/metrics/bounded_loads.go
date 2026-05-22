package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// BoundedLoads metrics. See design doc § BoundedLoads.
var (
	// ClusterBLAvgRPS is the current cluster-average RequestsPerSec used for BoundedLoads.
	ClusterBLAvgRPS = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_cluster_bl_avg_rps",
		Help: "Current cluster-average RequestsPerSec used for BoundedLoads.",
	})

	// ClusterBLThresholdHighRPS is the hot-enter threshold (avg*c).
	ClusterBLThresholdHighRPS = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_cluster_bl_threshold_high_rps",
		Help: "Hot-enter threshold (avg*c).",
	})

	// ClusterBLThresholdLowRPS is the hot-exit threshold (avg*c_low).
	ClusterBLThresholdLowRPS = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_cluster_bl_threshold_low_rps",
		Help: "Hot-exit threshold (avg*c_low).",
	})

	// ClusterBLHotNodes is the number of nodes currently in hot state.
	ClusterBLHotNodes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_cluster_bl_hot_nodes",
		Help: "Number of nodes currently in hot state.",
	})

	// ClusterBLSpilledWrites counts writes that spilled to a non-hot node due to BoundedLoads.
	// node label: node ID.
	ClusterBLSpilledWrites = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_cluster_bl_spilled_writes_total",
		Help: "Writes that spilled to a non-hot node due to BoundedLoads.",
	}, []string{"node"})

	// ClusterBLBypassedWrites counts writes where BL was bypassed because <k+m active nodes remained.
	ClusterBLBypassedWrites = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_cluster_bl_bypassed_writes_total",
		Help: "Writes where BL was bypassed because <k+m active nodes remained.",
	})

	// ClusterBLRerankedReads counts reads that swapped out a hot data shard to parity due to BoundedLoads.
	// node label: node ID.
	ClusterBLRerankedReads = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_cluster_bl_reranked_reads_total",
		Help: "Reads that swapped out a hot data shard to parity due to BoundedLoads.",
	}, []string{"node"})

	// ClusterBLBypassedReads counts reads where BL was bypassed because hot_count > m.
	ClusterBLBypassedReads = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_cluster_bl_bypassed_reads_total",
		Help: "Reads where BL was bypassed because hot_count > m.",
	})

	// ClusterBLHotStateTransitions counts transitions in/out of hot state per node.
	// node label: node ID; direction label: "enter" | "exit".
	ClusterBLHotStateTransitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_cluster_bl_hot_state_transitions_total",
		Help: "Transitions in/out of hot state per node.",
	}, []string{"node", "direction"})

	// ClusterPlacementSkipped counts per-reason placement candidates excluded (weight=0).
	// node label: node ID; reason label: exclusion reason.
	ClusterPlacementSkipped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_cluster_placement_skipped_total",
		Help: "Per-reason counter of placement candidates excluded (weight=0).",
	}, []string{"node", "reason"})

	// ClusterBLSnapshotRefresh counts BoundedLoads snapshot refresh outcomes.
	// result label: "fresh" | "singleflight_wait".
	ClusterBLSnapshotRefresh = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_cluster_bl_snapshot_refresh_total",
		Help: "BoundedLoads snapshot refresh outcomes.",
	}, []string{"result"})
)
