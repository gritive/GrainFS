package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestSyncCB_ThresholdHotReload covers the bug Slice 1 left in place: before
// state-minimization, BalancerCBThreshold rotation never reached an
// already-created breaker because the threshold was snapshotted at
// newCircuitBreaker() time. Post-batch-9, syncCB re-reads
// clusterCfg.BalancerCBThreshold() on every tick and passes it to
// cb.update(ns, thresholdPct). This test asserts that pipeline.
//
// Scope: BalancerProposer-level (not just the bare breaker — see
// circuit_breaker_test.go::TestCircuitBreaker_ThresholdHotReloadAcrossUpdates
// for the smaller-scope unit test). This one proves the wiring from
// clusterCfg through syncCB to existing breakers.
func TestSyncCB_ThresholdHotReload(t *testing.T) {
	cfg := testBalancerConfig()
	cfg.cbThreshold = 0.85 // 85%

	store := NewNodeStatsStore(1 * time.Minute)
	node := &mockRaftNode{state: 2, nodeID: "self", peerIDs: []string{"n2"}}
	p := NewBalancerProposer("self", store, node, cfg)

	peers := []NodeStats{
		{NodeID: "self"},
		{NodeID: "n2", DiskUsedPct: 88.0},
	}

	// First tick: 88% disk vs 85% threshold → CB open (block writes to n2).
	p.syncCB(peers)
	cb := p.getCB("n2")
	require.NotNil(t, cb, "expected breaker created for peer n2")
	require.False(t, cb.allow(), "expected CB open at 88%% vs 85%% threshold")

	// Hot-reload: raise threshold to 90%. Same disk, same breaker — CB should close.
	cfg.cbThreshold = 0.90
	p.syncCB(peers)
	require.True(t, p.getCB("n2").allow(), "expected CB closed after threshold raised to 90%%")

	// Lower threshold to 80%. CB should open again.
	cfg.cbThreshold = 0.80
	p.syncCB(peers)
	require.False(t, p.getCB("n2").allow(), "expected CB open after threshold lowered to 80%%")
}
