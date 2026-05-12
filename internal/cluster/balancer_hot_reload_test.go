package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBalancerProposer_HotReload_TriggerPct asserts that a cluster_config
// rotation of BalancerImbalanceTriggerPct (and BalancerImbalanceStopPct)
// reaches an already-running balancer's Active() decision on the very next
// tick. Slice 1 shipped this key's rev-propagation but did not verify the
// end-to-end behavioral hot-reload — this closes that gap (Task 16,
// v1.1 hardening).
//
// The production tick path (BalancerProposer.tickOnce) reads
// clusterCfg.BalancerImbalanceTriggerPct() / BalancerImbalanceStopPct()
// fresh every call, so mutating the fake cfg between ticks exercises the
// hot-reload pipeline.
func TestBalancerProposer_HotReload_TriggerPct(t *testing.T) {
	cfg := testBalancerConfig()
	cfg.imbalanceTriggerPct = 20.0
	cfg.imbalanceStopPct = 5.0

	store := NewNodeStatsStore(1 * time.Minute)
	// Two peers with a 15pp imbalance — below the 20% trigger initially.
	// JoinedAt left zero so anyNodeInGracePeriod() is false (no 1.5× boost).
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 60.0})
	store.Set(NodeStats{NodeID: "node-b", DiskUsedPct: 45.0})

	node := &mockRaftNode{state: 2, nodeID: "node-a", peerIDs: []string{"node-b"}}
	p := NewBalancerProposer("node-a", store, node, cfg)

	// Tick 1: 15% diff vs 20% trigger → stays inactive.
	p.tickOnce()
	require.False(t, p.Active(), "expected inactive at 15%% imbalance vs 20%% trigger")

	// Hot-reload: lower trigger to 10%. Same stats. Next tick should activate.
	cfg.imbalanceTriggerPct = 10.0
	p.tickOnce()
	require.True(t, p.Active(), "expected active after trigger lowered to 10%%")

	// Hot-reload: raise stop to 20% (above the 15pp current imbalance).
	// Next tick: active && diff < stopPct → deactivate.
	cfg.imbalanceStopPct = 20.0
	p.tickOnce()
	require.False(t, p.Active(), "expected inactive after stop raised above current imbalance")
}
