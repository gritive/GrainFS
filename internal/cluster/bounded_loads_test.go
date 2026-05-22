package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBoundedLoads_EmptySnapshot(t *testing.T) {
	store := NewNodeStatsStore(2 * time.Minute)
	bl := NewBoundedLoads(store, BoundedLoadsParams{C: 1.25, CLow: 1.0})

	snap := bl.Snapshot()
	assert.Equal(t, 0.0, snap.AvgRPS, "empty store: AvgRPS should be 0")
	assert.Empty(t, snap.HotSet, "empty store: HotSet should be empty")
}

func TestBoundedLoads_IsHotMissingNode(t *testing.T) {
	store := NewNodeStatsStore(2 * time.Minute)
	bl := NewBoundedLoads(store, BoundedLoadsParams{C: 1.25, CLow: 1.0})
	assert.False(t, bl.IsHot("missing-node"))
}

func TestBoundedLoads_RefreshComputesAvg(t *testing.T) {
	store := NewNodeStatsStore(2 * time.Minute)
	store.Set(NodeStats{NodeID: "n1", RequestsPerSec: 100})
	store.Set(NodeStats{NodeID: "n2", RequestsPerSec: 200})
	store.Set(NodeStats{NodeID: "n3", RequestsPerSec: 300})

	bl := NewBoundedLoads(store, BoundedLoadsParams{C: 1.25, CLow: 1.0})
	bl.Refresh()

	snap := bl.Snapshot()
	assert.Equal(t, 200.0, snap.AvgRPS)
	assert.Equal(t, 250.0, snap.HighThreshold)
	assert.Equal(t, 200.0, snap.LowThreshold)
	assert.Contains(t, snap.HotSet, "n3", "n3 (RPS 300 > high 250) should be in HotSet")
	assert.NotContains(t, snap.HotSet, "n1", "n1 (RPS 100 < high 250) should not be in HotSet")
	assert.NotContains(t, snap.HotSet, "n2", "n2 (RPS 200 < high 250) should not be in HotSet")
}

func TestBoundedLoads_Hysteresis(t *testing.T) {
	store := NewNodeStatsStore(2 * time.Minute)
	// Baseline: n1=120, n2=100, n3=80 → avg=100, high=125, low=100.
	store.Set(NodeStats{NodeID: "n1", RequestsPerSec: 120})
	store.Set(NodeStats{NodeID: "n2", RequestsPerSec: 100})
	store.Set(NodeStats{NodeID: "n3", RequestsPerSec: 80})

	bl := NewBoundedLoads(store, BoundedLoadsParams{C: 1.25, CLow: 1.0})
	bl.Refresh()
	assert.False(t, bl.IsHot("n1"), "baseline: n1 (120) should not be hot at high=125")

	// Spike n1 above high → enters hot.
	store.Set(NodeStats{NodeID: "n1", RequestsPerSec: 200})
	bl.Refresh()
	assert.True(t, bl.IsHot("n1"), "after spike: n1 should be hot")

	// Drop n1 to sticky zone (between low and high) → stays hot.
	store.Set(NodeStats{NodeID: "n1", RequestsPerSec: 115})
	bl.Refresh()
	assert.True(t, bl.IsHot("n1"), "sticky zone: n1 should stay hot until rps < low")

	// Drop n1 below low → exits hot.
	store.Set(NodeStats{NodeID: "n1", RequestsPerSec: 50})
	bl.Refresh()
	assert.False(t, bl.IsHot("n1"), "after recovery: n1 should not be hot")
}
