package cluster

import (
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/gritive/GrainFS/internal/metrics"
)

// BoundedLoadsParams configures hot-node detection.
type BoundedLoadsParams struct {
	C        float64       // hot 진입 multiplier (default 1.25)
	CLow     float64       // hot 탈출 multiplier (default 1.0, must be < C)
	MaxStale time.Duration // 절대 max snapshot age safety net
}

// BoundedLoadsSnapshot is an immutable view of cluster RPS state.
//
// HotSet is shared across all callers of Snapshot() — treat it as read-only.
// Mutation will corrupt the snapshot for concurrent readers. If you need to
// modify, make a defensive copy first.
type BoundedLoadsSnapshot struct {
	AvgRPS        float64
	HighThreshold float64
	LowThreshold  float64
	HotSet        map[string]struct{} // read-only; see godoc above
	ComputedAt    time.Time
	DataVersion   time.Time // max NodeStats.UpdatedAt seen at compute time
}

// BoundedLoads computes hot-node classification with hysteresis.
type BoundedLoads struct {
	store  *NodeStatsStore
	params BoundedLoadsParams
	snap   atomic.Pointer[BoundedLoadsSnapshot]
	sf     singleflight.Group
}

// NewBoundedLoads constructs a BoundedLoads bound to store with the given params.
func NewBoundedLoads(store *NodeStatsStore, params BoundedLoadsParams) *BoundedLoads {
	bl := &BoundedLoads{store: store, params: params}
	empty := &BoundedLoadsSnapshot{HotSet: map[string]struct{}{}}
	bl.snap.Store(empty)
	return bl
}

// Snapshot returns the current snapshot pointer (lock-free read).
func (bl *BoundedLoads) Snapshot() *BoundedLoadsSnapshot {
	return bl.snap.Load()
}

// IsHot reports whether nodeID is in the current hot set.
func (bl *BoundedLoads) IsHot(nodeID string) bool {
	snap := bl.snap.Load()
	_, ok := snap.HotSet[nodeID]
	return ok
}

// Refresh recomputes the snapshot from current NodeStatsStore state.
// Single-writer assumption: callers serialise (e.g. via singleflight in Task 4).
func (bl *BoundedLoads) Refresh() {
	stats := bl.store.GetAll()
	prev := bl.snap.Load()

	if len(stats) == 0 {
		// Empty store: reset to empty snapshot. Task 3+ owns sticky-aware expiry
		// behavior; currently we simply clear HotSet when all nodes are gone.
		empty := &BoundedLoadsSnapshot{
			HotSet:     map[string]struct{}{},
			ComputedAt: time.Now(),
		}
		bl.snap.Store(empty)
		return
	}

	var sum float64
	var maxUpdated time.Time
	for _, ns := range stats {
		sum += ns.RequestsPerSec
		if ns.UpdatedAt.After(maxUpdated) {
			maxUpdated = ns.UpdatedAt
		}
	}
	avg := sum / float64(len(stats))
	high := avg * bl.params.C
	low := avg * bl.params.CLow

	// prev is always non-nil: NewBoundedLoads stores an initial empty snapshot,
	// and Refresh always reads bl.snap.Load() before publishing.
	next := &BoundedLoadsSnapshot{
		AvgRPS:        avg,
		HighThreshold: high,
		LowThreshold:  low,
		HotSet:        computeHotSet(stats, prev, high, low),
		ComputedAt:    time.Now(),
		DataVersion:   maxUpdated,
	}

	// Emit gauges.
	metrics.ClusterBLAvgRPS.Set(avg)
	metrics.ClusterBLThresholdHighRPS.Set(high)
	metrics.ClusterBLThresholdLowRPS.Set(low)
	metrics.ClusterBLHotNodes.Set(float64(len(next.HotSet)))

	// Emit transition counters by diffing prev vs next.HotSet.
	if prev != nil {
		for id := range next.HotSet {
			if _, was := prev.HotSet[id]; !was {
				metrics.ClusterBLHotStateTransitions.WithLabelValues(id, "enter").Inc()
			}
		}
		for id := range prev.HotSet {
			if _, still := next.HotSet[id]; !still {
				metrics.ClusterBLHotStateTransitions.WithLabelValues(id, "exit").Inc()
			}
		}
	}

	bl.snap.Store(next)
}

// RefreshIfStale recomputes the snapshot only if the underlying NodeStatsStore
// has advanced (max UpdatedAt > snapshot.DataVersion) or the snapshot has aged
// past params.MaxStale. Concurrent callers coalesce via singleflight.
func (bl *BoundedLoads) RefreshIfStale() {
	cur := bl.snap.Load()
	if !bl.shouldRefresh(cur) {
		return
	}
	_, _, _ = bl.sf.Do("refresh", func() (interface{}, error) {
		// Re-check under singleflight to avoid duplicate work after wait.
		cur := bl.snap.Load()
		if !bl.shouldRefresh(cur) {
			metrics.ClusterBLSnapshotRefresh.WithLabelValues("singleflight_wait").Inc()
			return nil, nil
		}
		bl.Refresh()
		metrics.ClusterBLSnapshotRefresh.WithLabelValues("fresh").Inc()
		return nil, nil
	})
}

func (bl *BoundedLoads) shouldRefresh(cur *BoundedLoadsSnapshot) bool {
	if cur == nil || cur.ComputedAt.IsZero() {
		return true
	}
	if bl.params.MaxStale > 0 && time.Since(cur.ComputedAt) > bl.params.MaxStale {
		return true
	}
	// dataVersion advanced?
	stats := bl.store.GetAll()
	var maxUpdated time.Time
	for _, ns := range stats {
		if ns.UpdatedAt.After(maxUpdated) {
			maxUpdated = ns.UpdatedAt
		}
	}
	return maxUpdated.After(cur.DataVersion)
}

// computeHotSet applies the hysteresis state machine:
//   - rps > high     → hot (regardless of prev state)
//   - rps < low      → not hot (regardless of prev state)
//   - low ≤ rps ≤ high → sticky: keep prev state
//
// prev is non-nil — NewBoundedLoads stores an initial empty snapshot, and
// Refresh always reads bl.snap.Load() before publishing.
func computeHotSet(stats []NodeStats, prev *BoundedLoadsSnapshot, high, low float64) map[string]struct{} {
	out := make(map[string]struct{}, len(stats))
	prevHot := map[string]struct{}{}
	if prev != nil {
		prevHot = prev.HotSet
	}
	for _, ns := range stats {
		switch {
		case ns.RequestsPerSec > high:
			out[ns.NodeID] = struct{}{}
		case ns.RequestsPerSec < low:
			// not hot, omit
		default:
			// sticky zone: keep previous hot state
			if _, wasHot := prevHot[ns.NodeID]; wasHot {
				out[ns.NodeID] = struct{}{}
			}
		}
	}
	return out
}
