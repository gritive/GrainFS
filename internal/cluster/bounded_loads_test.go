package cluster

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/gritive/GrainFS/internal/metrics"
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

func TestBoundedLoads_RefreshIfStaleSingleflight(t *testing.T) {
	store := NewNodeStatsStore(2 * time.Minute)
	store.Set(NodeStats{NodeID: "n1", RequestsPerSec: 100})

	bl := NewBoundedLoads(store, BoundedLoadsParams{C: 1.25, CLow: 1.0, MaxStale: 60 * time.Second})
	bl.Refresh()
	first := bl.Snapshot()

	// dataVersion 동일 → 재계산 skip.
	bl.RefreshIfStale()
	second := bl.Snapshot()
	assert.Same(t, first, second, "dataVersion unchanged: snapshot pointer should be reused")

	// 데이터 갱신 → reload.
	store.Set(NodeStats{NodeID: "n2", RequestsPerSec: 200})
	bl.RefreshIfStale()
	third := bl.Snapshot()
	assert.NotSame(t, first, third, "dataVersion changed: snapshot should be refreshed")
	assert.Equal(t, 150.0, third.AvgRPS)
}

func TestBoundedLoads_RefreshEmitsMetrics(t *testing.T) {
	store := NewNodeStatsStore(2 * time.Minute)
	store.Set(NodeStats{NodeID: "m1", RequestsPerSec: 100})
	store.Set(NodeStats{NodeID: "m2", RequestsPerSec: 200})
	bl := NewBoundedLoads(store, BoundedLoadsParams{C: 1.25, CLow: 1.0})
	bl.Refresh()
	assert.Equal(t, 150.0, testutil.ToFloat64(metrics.ClusterBLAvgRPS))
	// n2 (200) > high (187.5) → 1 hot node
	assert.Equal(t, 1.0, testutil.ToFloat64(metrics.ClusterBLHotNodes))
}

func TestBoundedLoads_HotTransitionCount(t *testing.T) {
	// Use unique node IDs to avoid label pollution from other tests.
	// Two baseline nodes keep avg below the spiked node's rps so hot detection fires.
	baselineA := "trans-baseline-a"
	baselineB := "trans-baseline-b"
	target := "trans-target-node"

	before := testutil.ToFloat64(metrics.ClusterBLHotStateTransitions.WithLabelValues(target, "enter"))

	store := NewNodeStatsStore(2 * time.Minute)
	store.Set(NodeStats{NodeID: baselineA, RequestsPerSec: 100})
	store.Set(NodeStats{NodeID: baselineB, RequestsPerSec: 100})
	store.Set(NodeStats{NodeID: target, RequestsPerSec: 100})
	bl := NewBoundedLoads(store, BoundedLoadsParams{C: 1.25, CLow: 1.0})
	bl.Refresh()
	// avg=100, high=125 → target (100) not hot

	// Spike target well above high threshold: avg≈(100+100+500)/3≈233, high≈292 → 500>292 → enter
	store.Set(NodeStats{NodeID: target, RequestsPerSec: 500})
	bl.Refresh()

	after := testutil.ToFloat64(metrics.ClusterBLHotStateTransitions.WithLabelValues(target, "enter"))
	assert.Equal(t, 1.0, after-before)
}
