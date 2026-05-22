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
	store  *NodeStatsStore // dormant until Refresh (Task 2 of the plan)
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
