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

// mutableParams lets a single test instance mutate BL thresholds mid-flight.
// Mirrors the production pattern where *ClusterConfig is patched via raft and
// the change becomes visible on the next BoundedLoads.Refresh.
type mutableParams struct {
	c, cLow  float64
	maxStale time.Duration
}

func (p *mutableParams) BoundedLoadsC() float64                 { return p.c }
func (p *mutableParams) BoundedLoadsCLow() float64              { return p.cLow }
func (p *mutableParams) BoundedLoadsMaxStaleTTL() time.Duration { return p.maxStale }

func TestBoundedLoads_LiveCTuning(t *testing.T) {
	// Verify high/low thresholds reflect cfg values live (no snapshot at construction).
	// Assert thresholds directly to avoid hysteresis sticky-band interference.
	store := NewNodeStatsStore(2 * time.Minute)
	store.Set(NodeStats{NodeID: "live-c-a", RequestsPerSec: 100})
	store.Set(NodeStats{NodeID: "live-c-b", RequestsPerSec: 300})
	// avg = 200

	p := &mutableParams{c: 1.25, cLow: 1.0, maxStale: time.Minute}
	bl := NewBoundedLoads(store, p)
	bl.Refresh()
	snap1 := bl.Snapshot()
	assert.Equal(t, 250.0, snap1.HighThreshold, "c=1.25: high=200*1.25=250")
	assert.Equal(t, 200.0, snap1.LowThreshold, "cLow=1.0: low=200*1.0=200")

	// Operator dials c and cLow up. No restart, no new BoundedLoads — just
	// mutate the shared cfg view (in production: raft cluster_config patch).
	p.c = 1.5
	p.cLow = 1.1
	bl.Refresh()
	snap2 := bl.Snapshot()
	assert.Equal(t, 300.0, snap2.HighThreshold, "c=1.5: high=200*1.5=300 (live-tuned)")
	assert.InDelta(t, 220.0, snap2.LowThreshold, 0.0001, "cLow=1.1: low=200*1.1=220 (live-tuned)")
}

func TestBoundedLoads_LiveMaxStaleTuning(t *testing.T) {
	// Verify RefreshIfStale honors the live MaxStale value.
	store := NewNodeStatsStore(2 * time.Minute)
	store.Set(NodeStats{NodeID: "live-ms-only", RequestsPerSec: 100})

	p := &mutableParams{c: 1.25, cLow: 1.0, maxStale: time.Hour}
	bl := NewBoundedLoads(store, p)
	bl.Refresh()
	first := bl.Snapshot()

	// Backdate the snapshot to 90s ago so a 60s MaxStale would trigger refresh.
	stale := *first
	stale.ComputedAt = time.Now().Add(-90 * time.Second)
	bl.snap.Store(&stale)

	// MaxStale=1h → not stale, no refresh.
	bl.RefreshIfStale()
	assert.Same(t, &stale, bl.Snapshot(), "MaxStale=1h: snapshot reused")

	// Operator dials MaxStale down. No restart.
	p.maxStale = 60 * time.Second
	bl.RefreshIfStale()
	assert.NotSame(t, &stale, bl.Snapshot(), "MaxStale=60s: stale snapshot refreshed (live-tuned)")
}
