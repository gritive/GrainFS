package cluster

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// mockRaftNode captures Propose calls for balancer testing.
type mockRaftNode struct {
	mu          sync.Mutex
	proposed    [][]byte
	state       int // 0=Follower, 2=Leader
	nodeID      string
	peerIDs     []string
	transferred bool
}

func (m *mockRaftNode) Propose(data []byte) error {
	m.mu.Lock()
	m.proposed = append(m.proposed, data)
	m.mu.Unlock()
	return nil
}

func (m *mockRaftNode) IsLeader() bool { return m.state == 2 }
func (m *mockRaftNode) NodeID() string  { return m.nodeID }
func (m *mockRaftNode) PeerIDs() []string { return m.peerIDs }
func (m *mockRaftNode) TransferLeadership() error {
	m.mu.Lock()
	m.transferred = true
	m.mu.Unlock()
	return nil
}

func (m *mockRaftNode) ProposedLen() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.proposed)
}

func (m *mockRaftNode) ProposedAt(i int) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.proposed[i]
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
	p.SetObjectPicker(&mockObjectPicker{bucket: "b", key: "k", ok: true})
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
	p.SetObjectPicker(&mockObjectPicker{bucket: "b", key: "k", ok: true})
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
	p.SetObjectPicker(&mockObjectPicker{bucket: "b", key: "k", ok: true})
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

func TestSelectPeerByLoad_EvenCountMedian(t *testing.T) {
	// With 4 nodes (even count), loads[len/2] = loads[2] which is the upper-middle.
	// The median of [50, 100, 150, 200] should be 125 (average of middle two).
	// Current implementation uses loads[2]=150 as median — this test documents the behavior.
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "n1", RequestsPerSec: 50.0})
	store.Set(NodeStats{NodeID: "n2", RequestsPerSec: 100.0})
	store.Set(NodeStats{NodeID: "n3", RequestsPerSec: 150.0})
	store.Set(NodeStats{NodeID: "n4", RequestsPerSec: 200.0})

	// self=n4 with 200 rps. threshold=1.3. loads sorted: [50,100,150,200], loads[2]=150 (upper-middle).
	// 200 > 150*1.3=195 → overloaded → should redirect
	peer, ok := selectPeerByLoad(store, "n4", 1.3)
	require.True(t, ok, "n4 at 200 rps should exceed 150*1.3=195 median threshold")
	assert.Equal(t, "n1", peer, "should redirect to lightest peer n1")
}

func TestSelectPeerByLoad_SingleNodeBalancer(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "n1", RequestsPerSec: 999.0})

	_, ok := selectPeerByLoad(store, "n1", 1.3)
	assert.False(t, ok, "single node cannot redirect")
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

// --- Grace period tests ---

func TestBalancerProposer_GracePeriod_RelaxesTrigger(t *testing.T) {
	// node-b joined recently: its JoinedAt is within GracePeriod (10m default).
	// Normal trigger=20%; with any node in grace period the effective trigger is
	// ImbalanceTriggerPct * 1.5 = 30%.
	// Imbalance is 25% (between 20% and 30%), so proposal should be skipped.
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 75.0, DiskAvailBytes: 20 << 30})
	store.Set(NodeStats{NodeID: "node-b", DiskUsedPct: 50.0, DiskAvailBytes: 100 << 30,
		JoinedAt: time.Now().Add(-1 * time.Minute)}) // recently joined

	node := &mockRaftNode{state: 2, nodeID: "node-a", peerIDs: []string{"node-b"}}
	cfg := testBalancerConfig()
	cfg.GracePeriod = 10 * time.Minute // grace period active for 10 min after join

	p := NewBalancerProposer("node-a", store, node, cfg)
	p.SetObjectPicker(&mockObjectPicker{bucket: "b", key: "k", ok: true})
	p.tickOnce(context.Background())

	assert.Empty(t, node.proposed, "25% imbalance below relaxed 30% trigger during grace period: no proposal expected")
}

func TestBalancerProposer_GracePeriod_FiresAboveRelaxedTrigger(t *testing.T) {
	// node-b joined recently; imbalance is 40% which exceeds the relaxed 30% trigger.
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 90.0, DiskAvailBytes: 5 << 30})
	store.Set(NodeStats{NodeID: "node-b", DiskUsedPct: 50.0, DiskAvailBytes: 100 << 30,
		JoinedAt: time.Now().Add(-1 * time.Minute)})

	node := &mockRaftNode{state: 2, nodeID: "node-a", peerIDs: []string{"node-b"}}
	cfg := testBalancerConfig()
	cfg.GracePeriod = 10 * time.Minute

	p := NewBalancerProposer("node-a", store, node, cfg)
	p.SetObjectPicker(&mockObjectPicker{bucket: "b", key: "k", ok: true})
	p.tickOnce(context.Background())

	assert.NotEmpty(t, node.proposed, "40% imbalance exceeds relaxed 30% trigger: proposal expected")
}

func TestBalancerProposer_GracePeriod_ExpiredNodeUseNormalTrigger(t *testing.T) {
	// node-b joined 15 min ago — past the 10 min grace period.
	// Imbalance is 25%, which is above the normal 20% trigger, so proposal expected.
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 75.0, DiskAvailBytes: 20 << 30})
	store.Set(NodeStats{NodeID: "node-b", DiskUsedPct: 50.0, DiskAvailBytes: 100 << 30,
		JoinedAt: time.Now().Add(-15 * time.Minute)}) // past grace period

	node := &mockRaftNode{state: 2, nodeID: "node-a", peerIDs: []string{"node-b"}}
	cfg := testBalancerConfig()
	cfg.GracePeriod = 10 * time.Minute

	p := NewBalancerProposer("node-a", store, node, cfg)
	p.SetObjectPicker(&mockObjectPicker{bucket: "b", key: "k", ok: true})
	p.tickOnce(context.Background())

	assert.NotEmpty(t, node.proposed, "grace period expired: normal 20% trigger applies, 25% should fire")
}

// TestBalancerProposer_InflightDedup verifies that the same object is not
// proposed again on the next tick while a migration is already in flight.
func TestBalancerProposer_InflightDedup(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 80})
	store.Set(NodeStats{NodeID: "node-b", DiskUsedPct: 10})

	node := &mockRaftNode{state: 2, nodeID: "node-a", peerIDs: []string{"node-b"}}
	cfg := testBalancerConfig()
	p := NewBalancerProposer("node-a", store, node, cfg)
	p.SetObjectPicker(&mockObjectPicker{bucket: "b", key: "k", ok: true})

	// First tick: migration proposed and added to inflight.
	p.tickOnce(context.Background())
	require.Len(t, node.proposed, 1, "first tick must propose")

	// Second tick: same object still in inflight → must be skipped.
	p.tickOnce(context.Background())
	assert.Len(t, node.proposed, 1, "second tick must not re-propose while migration is inflight")
}

// TestLocalObjectPicker_NestedKey verifies that PickObjectOnSrcNode finds objects
// whose S3 key contains '/' (stored as nested directories by ShardService).
func TestLocalObjectPicker_NestedKey(t *testing.T) {
	dir := t.TempDir()
	// Simulate ShardService on-disk layout: {shardsDir}/{bucket}/{key}/shard_0
	// Key "photos/2024/img.jpg" → nested dirs
	keyPath := filepath.Join(dir, "mybucket", "photos", "2024", "img.jpg")
	require.NoError(t, os.MkdirAll(keyPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(keyPath, "shard_0"), []byte("data"), 0o644))

	picker := NewLocalObjectPicker(dir)
	bucket, key, versionID, ok := picker.PickObjectOnSrcNode("any")

	require.True(t, ok, "should find the nested-key object")
	assert.Equal(t, "mybucket", bucket)
	assert.Equal(t, filepath.Join("photos", "2024", "img.jpg"), key)
	assert.Empty(t, versionID)
}
