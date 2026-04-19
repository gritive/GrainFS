package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// testBalancerProposerForWarmup creates a BalancerProposer with stale detection via NodeStats.UpdatedAt.
func testBalancerProposerForWarmup(store *NodeStatsStore, peers []string, timeout time.Duration) *BalancerProposer {
	node := &mockRaftNode{nodeID: "self", peerIDs: peers, state: 2}
	cfg := testBalancerConfig()
	cfg.WarmupTimeout = timeout
	cfg.PeerSeenWindow = 2 * cfg.GossipInterval
	p := NewBalancerProposer("self", store, node, cfg)
	return p
}

// TestWarmupComplete_AllPeersRecent: all peers have recent gossip → warmup done.
func TestWarmupComplete_AllPeersRecent(t *testing.T) {
	store := NewNodeStatsStore(time.Minute)
	peers := []string{"n1", "n2"}

	// Seed self and all peers with fresh stats
	for _, id := range append(peers, "self") {
		store.Set(NodeStats{NodeID: id, DiskUsedPct: 10})
	}

	p := testBalancerProposerForWarmup(store, peers, 30*time.Second)
	assert.True(t, p.warmupComplete(peers), "all peers recent → warmup complete")
}

// TestWarmupComplete_StaleAllPeers: peers have stale gossip → warmup NOT complete (timeout not reached).
func TestWarmupComplete_StaleAllPeers(t *testing.T) {
	ttl := 500 * time.Millisecond
	store := NewNodeStatsStore(ttl)
	peers := []string{"n1", "n2"}

	// Seed self
	store.Set(NodeStats{NodeID: "self", DiskUsedPct: 10})
	// Seed n1 as stale (will expire before warmup check)
	store.Set(NodeStats{NodeID: "n1", DiskUsedPct: 10})
	// n2 never gossiped

	p := testBalancerProposerForWarmup(store, peers, 30*time.Second)

	// Wait for n1's entry to expire
	time.Sleep(ttl + 20*time.Millisecond)

	// store.Len() < len(peers)+1 AND timeout not reached → not complete
	assert.False(t, p.warmupComplete(peers), "stale/missing peers → warmup incomplete")
}

// TestWarmupComplete_PeerNeverGossips: peer that never gossiped → timeout path returns true.
func TestWarmupComplete_PeerNeverGossips(t *testing.T) {
	store := NewNodeStatsStore(time.Minute)
	peers := []string{"n1"}

	// Only self in store; n1 never gossiped
	store.Set(NodeStats{NodeID: "self", DiskUsedPct: 10})

	// Very short timeout
	p := testBalancerProposerForWarmup(store, peers, 1*time.Millisecond)
	p.startedAt = p.startedAt.Add(-time.Second) // simulate timeout elapsed

	assert.True(t, p.warmupComplete(peers), "timeout elapsed → warmup complete regardless")
}
