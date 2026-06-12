package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/gossip"
)

// TestLoadSupplyChain_CollectorFeedsBoundedLoads is the Phase 6 S6-2 dynamic
// confirmation that the request-rate producer actually lights up the consumer.
//
// Before S6-2 RequestsPerSec had no production producer, so AvgRPS was always 0
// and BoundedLoads.HotSet was always empty — hot-node read reranking was inert.
// This drives the real RequestRateCollector against a gossip.NodeStatsStore, then a real
// BoundedLoads reading that same store, and asserts a genuinely overloaded node is
// detected as hot. Were the collector→store→BoundedLoads chain broken, HotSet would
// stay empty and IsHot would return false (the pre-S6-2 behavior).
func TestLoadSupplyChain_CollectorFeedsBoundedLoads(t *testing.T) {
	store := gossip.NewNodeStatsStore(1 * time.Minute)
	// Three nodes present in the store (gossip would populate peers in production;
	// here we seed them directly, then let the collector own each node's RPS field).
	for _, id := range []string{"hot", "cool-a", "cool-b"} {
		store.Set(gossip.NodeStats{NodeID: id, DiskAvailBytes: 1 << 30})
	}

	// Per-node request-rate collectors, each sampling that node's own counter.
	counts := map[string]float64{"hot": 0, "cool-a": 0, "cool-b": 0}
	collectors := map[string]*RequestRateCollector{}
	t0 := time.Now()
	for id := range counts {
		nodeID := id
		c := NewRequestRateCollector(nodeID, store, time.Second, func() float64 { return counts[nodeID] })
		c.seed(t0)
		collectors[nodeID] = c
	}

	// Over a 10s window: hot node served 5000 reqs (500 rps), cool nodes 500 (50 rps).
	counts["hot"] = 5000
	counts["cool-a"] = 500
	counts["cool-b"] = 500
	for id, c := range collectors {
		c.collect(t0.Add(10 * time.Second))
		got, ok := store.Get(id)
		require.True(t, ok)
		_ = got
	}

	hotNS, _ := store.Get("hot")
	require.InDelta(t, 500.0, hotNS.RequestsPerSec, 1e-6, "collector must have produced the hot node's RPS")

	bl := NewBoundedLoads(store, BoundedLoadsParams{C: 1.25, CLow: 1.0})
	bl.Refresh()

	assert.True(t, bl.IsHot("hot"), "supply chain must surface the overloaded node as hot (was inert pre-S6-2)")
	assert.False(t, bl.IsHot("cool-a"), "lightly loaded node must not be hot")
	assert.False(t, bl.IsHot("cool-b"), "lightly loaded node must not be hot")
}
