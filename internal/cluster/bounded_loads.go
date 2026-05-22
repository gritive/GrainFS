package cluster

import (
	"sync/atomic"
	"time"
)

// BoundedLoadsParams configures hot-node detection.
type BoundedLoadsParams struct {
	C        float64       // hot 진입 multiplier (default 1.25)
	CLow     float64       // hot 탈출 multiplier (default 1.0, must be < C)
	MaxStale time.Duration // 절대 max snapshot age safety net
}

// BoundedLoadsSnapshot is an immutable view of cluster RPS state.
type BoundedLoadsSnapshot struct {
	AvgRPS        float64
	HighThreshold float64
	LowThreshold  float64
	HotSet        map[string]struct{}
	ComputedAt    time.Time
	DataVersion   time.Time // max NodeStats.UpdatedAt seen at compute time
}

// BoundedLoads computes hot-node classification with hysteresis.
type BoundedLoads struct {
	store  *NodeStatsStore
	params BoundedLoadsParams
	snap   atomic.Pointer[BoundedLoadsSnapshot]
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

	next := &BoundedLoadsSnapshot{
		AvgRPS:        avg,
		HighThreshold: high,
		LowThreshold:  low,
		HotSet:        computeHotSet(stats, prev, high, low),
		ComputedAt:    time.Now(),
		DataVersion:   maxUpdated,
	}
	bl.snap.Store(next)
}

// computeHotSet applies the hysteresis state machine.
// Task 2: initial implementation — rps > high → hot. Task 3 adds sticky band.
func computeHotSet(stats []NodeStats, _ *BoundedLoadsSnapshot, high, _ float64) map[string]struct{} {
	out := make(map[string]struct{}, len(stats))
	for _, ns := range stats {
		if ns.RequestsPerSec > high {
			out[ns.NodeID] = struct{}{}
		}
	}
	return out
}
