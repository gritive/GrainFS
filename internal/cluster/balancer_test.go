package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// mockRaftNode captures Propose calls for balancer testing.
type mockRaftNode struct {
	proposed     [][]byte
	state        int // 0=Follower, 2=Leader
	nodeID       string
	peerIDs      []string
	transferred  bool
}

func (m *mockRaftNode) Propose(data []byte) error {
	m.proposed = append(m.proposed, data)
	return nil
}

func (m *mockRaftNode) IsLeader() bool { return m.state == 2 }
func (m *mockRaftNode) NodeID() string  { return m.nodeID }
func (m *mockRaftNode) PeerIDs() []string { return m.peerIDs }
func (m *mockRaftNode) TransferLeadership() error {
	m.transferred = true
	return nil
}

// testBalancerConfig returns a BalancerConfig with aggressive timings for testing.
func testBalancerConfig() BalancerConfig {
	return BalancerConfig{
		GossipInterval:       10 * time.Millisecond,
		WarmupTimeout:        50 * time.Millisecond,
		ImbalanceTriggerPct:  20.0,
		ImbalanceStopPct:     5.0,
		MigrationRate:        100, // migrations per second in tests
		LeaderTenureMin:      0,   // no tenure requirement in tests
		LeaderLoadThreshold:  1.3, // trigger if leader > median × 1.3
	}
}

// --- BalancerProposer tests ---

func TestBalancerProposer_NoActionWhenBalanced(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 50.0})
	store.Set(NodeStats{NodeID: "node-b", DiskUsedPct: 55.0}) // 5% diff — below 20% trigger

	node := &mockRaftNode{state: 2, nodeID: "node-a", peerIDs: []string{"node-b"}}
	cfg := testBalancerConfig()

	p := NewBalancerProposer("node-a", store, node, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	p.tickOnce(ctx)

	assert.Empty(t, node.proposed, "should not propose when disk diff < 20%")
}

func TestBalancerProposer_ProposesMigrationWhenImbalanced(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 80.0, DiskAvailBytes: 10 << 30})
	store.Set(NodeStats{NodeID: "node-b", DiskUsedPct: 40.0, DiskAvailBytes: 100 << 30}) // 40% diff

	node := &mockRaftNode{state: 2, nodeID: "node-a", peerIDs: []string{"node-b"}}
	cfg := testBalancerConfig()

	p := NewBalancerProposer("node-a", store, node, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	p.tickOnce(ctx)

	require.NotEmpty(t, node.proposed, "should propose migration when imbalanced")

	var cmd clusterpb.Command
	require.NoError(t, proto.Unmarshal(node.proposed[0], &cmd))
	assert.Equal(t, uint32(CmdMigrateShard), cmd.Type)
}

func TestBalancerProposer_OnlyLeaderProposes(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 80.0})
	store.Set(NodeStats{NodeID: "node-b", DiskUsedPct: 40.0})

	// follower node
	node := &mockRaftNode{state: 0, nodeID: "node-a", peerIDs: []string{"node-b"}}
	cfg := testBalancerConfig()

	p := NewBalancerProposer("node-a", store, node, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	p.tickOnce(ctx)

	assert.Empty(t, node.proposed, "follower should not propose migration")
}

func TestBalancerProposer_WarmupGate(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	// only 1 of 2 peers seen — warm-up not complete
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 80.0})

	node := &mockRaftNode{state: 2, nodeID: "node-a", peerIDs: []string{"node-b", "node-c"}}
	cfg := testBalancerConfig()

	p := NewBalancerProposer("node-a", store, node, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	p.tickOnce(ctx)

	assert.Empty(t, node.proposed, "should wait for all peers before proposing (warm-up gate)")
}

func TestBalancerProposer_WarmupBypassAfterTimeout(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	// only 1 of 2 peers seen — warm-up not complete, but timeout expired
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 80.0})
	store.Set(NodeStats{NodeID: "node-b", DiskUsedPct: 40.0}) // node-c missing

	node := &mockRaftNode{state: 2, nodeID: "node-a", peerIDs: []string{"node-b", "node-c"}}
	cfg := testBalancerConfig()
	cfg.WarmupTimeout = 1 * time.Millisecond // immediate timeout

	p := NewBalancerProposer("node-a", store, node, cfg)
	time.Sleep(5 * time.Millisecond) // let warm-up timeout pass

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	p.tickOnce(ctx)

	// With available peers showing 40% imbalance, should propose
	require.NotEmpty(t, node.proposed, "should proceed after warm-up timeout")
}

func TestBalancerProposer_HysteresisStopsAtLowThreshold(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 53.0})
	store.Set(NodeStats{NodeID: "node-b", DiskUsedPct: 50.0}) // 3% diff — below stop threshold

	node := &mockRaftNode{state: 2, nodeID: "node-a", peerIDs: []string{"node-b"}}
	cfg := testBalancerConfig()

	p := NewBalancerProposer("node-a", store, node, cfg)
	// Simulate already-triggered state (imbalance was above 20%, now dropped to 3%)
	p.active = true

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	p.tickOnce(ctx)

	assert.Empty(t, node.proposed, "should stop migration when diff < 5% (hysteresis)")
	assert.False(t, p.active, "active flag should be cleared below stop threshold")
}

func TestBalancerProposer_MigrationTargetIsLightestNode(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 80.0, DiskAvailBytes: 10 << 30})
	store.Set(NodeStats{NodeID: "node-b", DiskUsedPct: 60.0, DiskAvailBytes: 50 << 30})
	store.Set(NodeStats{NodeID: "node-c", DiskUsedPct: 30.0, DiskAvailBytes: 200 << 30}) // lightest

	node := &mockRaftNode{state: 2, nodeID: "node-a", peerIDs: []string{"node-b", "node-c"}}
	cfg := testBalancerConfig()

	p := NewBalancerProposer("node-a", store, node, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	p.tickOnce(ctx)

	require.NotEmpty(t, node.proposed)
	var cmd clusterpb.Command
	require.NoError(t, proto.Unmarshal(node.proposed[0], &cmd))
	var migrate clusterpb.MigrateShardCmd
	require.NoError(t, proto.Unmarshal(cmd.Data, &migrate))
	assert.Equal(t, "node-c", migrate.DstNode, "should migrate to lightest node")
}

// --- selectLightestPeer tests ---

func TestSelectLightestPeer_ReturnsLowestDisk(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 80.0})
	store.Set(NodeStats{NodeID: "node-b", DiskUsedPct: 40.0})
	store.Set(NodeStats{NodeID: "node-c", DiskUsedPct: 60.0})

	peer, ok := selectLightestPeer(store, "node-a")
	require.True(t, ok)
	assert.Equal(t, "node-b", peer)
}

func TestSelectLightestPeer_ExcludesSelf(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 10.0}) // would be lightest, but is self
	store.Set(NodeStats{NodeID: "node-b", DiskUsedPct: 50.0})

	peer, ok := selectLightestPeer(store, "node-a")
	require.True(t, ok)
	assert.Equal(t, "node-b", peer)
}

func TestSelectLightestPeer_ReturnsFalseWhenNoPeers(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 80.0})

	_, ok := selectLightestPeer(store, "node-a")
	assert.False(t, ok)
}

// --- imbalancePct tests ---

func TestImbalancePct_Correct(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 80.0})
	store.Set(NodeStats{NodeID: "node-b", DiskUsedPct: 50.0})

	pct := imbalancePct(store)
	assert.InDelta(t, 30.0, pct, 0.001)
}

func TestImbalancePct_SingleNode(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 80.0})

	pct := imbalancePct(store)
	assert.Equal(t, 0.0, pct, "single node has no imbalance")
}

// --- Leader load balancing tests ---

func TestLeaderBalance_TransfersWhenOverloaded(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "leader", RequestsPerSec: 300.0})
	store.Set(NodeStats{NodeID: "peer-a", RequestsPerSec: 50.0})
	store.Set(NodeStats{NodeID: "peer-b", RequestsPerSec: 80.0})

	node := &mockRaftNode{state: 2, nodeID: "leader", peerIDs: []string{"peer-a", "peer-b"}}
	cfg := testBalancerConfig() // LeaderLoadThreshold: 1.3, LeaderTenureMin: 0
	p := NewBalancerProposer("leader", store, node, cfg)
	p.startedAt = time.Now().Add(-cfg.WarmupTimeout - time.Second) // warmup done

	p.tickOnce(context.Background())

	assert.True(t, node.transferred, "leader overloaded: expected TransferLeadership")
}

func TestLeaderBalance_NoTransferWhenBalanced(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "leader", RequestsPerSec: 100.0})
	store.Set(NodeStats{NodeID: "peer-a", RequestsPerSec: 90.0})
	store.Set(NodeStats{NodeID: "peer-b", RequestsPerSec: 110.0})

	node := &mockRaftNode{state: 2, nodeID: "leader", peerIDs: []string{"peer-a", "peer-b"}}
	cfg := testBalancerConfig()
	p := NewBalancerProposer("leader", store, node, cfg)
	p.startedAt = time.Now().Add(-cfg.WarmupTimeout - time.Second)

	p.tickOnce(context.Background())

	assert.False(t, node.transferred, "leader load within threshold: no transfer expected")
}

func TestLeaderBalance_NoTransferWhenFollower(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", RequestsPerSec: 300.0})
	store.Set(NodeStats{NodeID: "node-b", RequestsPerSec: 50.0})

	node := &mockRaftNode{state: 0, nodeID: "node-a", peerIDs: []string{"node-b"}} // follower
	cfg := testBalancerConfig()
	p := NewBalancerProposer("node-a", store, node, cfg)
	p.startedAt = time.Now().Add(-cfg.WarmupTimeout - time.Second)

	p.tickOnce(context.Background())

	assert.False(t, node.transferred, "follower: no transfer expected")
}
