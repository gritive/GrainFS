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

// TestBalancerIntegration_ProposesOnImbalance verifies that BalancerProposer.Run()
// automatically proposes CmdMigrateShard when disk usage is imbalanced.
func TestBalancerIntegration_ProposesOnImbalance(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "leader", DiskUsedPct: 80.0, DiskAvailBytes: 10 << 30})
	store.Set(NodeStats{NodeID: "peer-a", DiskUsedPct: 30.0, DiskAvailBytes: 200 << 30})

	node := &mockRaftNode{
		state:   2, // leader
		nodeID:  "leader",
		peerIDs: []string{"peer-a"},
	}

	cfg := BalancerConfig{
		GossipInterval:      10 * time.Millisecond,
		WarmupTimeout:       1 * time.Millisecond, // immediate
		ImbalanceTriggerPct: 20.0,
		ImbalanceStopPct:    5.0,
		MigrationRate:       1,
		LeaderTenureMin:     0,
		LeaderLoadThreshold: 1.3,
	}

	p := NewBalancerProposer("leader", store, node, cfg)
	p.SetObjectPicker(&mockObjectPicker{bucket: "b", key: "k", ok: true})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go p.Run(ctx)

	// Wait for at least one proposal
	require.Eventually(t, func() bool {
		return node.ProposedLen() > 0
	}, 400*time.Millisecond, 10*time.Millisecond, "timeout: no CmdMigrateShard proposed within 400ms")

	require.Greater(t, node.ProposedLen(), 0)
	var cmd clusterpb.Command
	require.NoError(t, proto.Unmarshal(node.ProposedAt(0), &cmd))
	assert.Equal(t, uint32(CmdMigrateShard), cmd.Type, "should propose CmdMigrateShard")

	var migrate clusterpb.MigrateShardCmd
	require.NoError(t, proto.Unmarshal(cmd.Data, &migrate))
	assert.Equal(t, "leader", migrate.SrcNode)
	assert.Equal(t, "peer-a", migrate.DstNode)
}

// TestBalancerIntegration_ExecutorNotifyLoop verifies the MigrationExecutor full loop:
// Execute() copies shards, proposes CmdMigrationDone, waits for NotifyCommit, then deletes src.
func TestBalancerIntegration_ExecutorNotifyLoop(t *testing.T) {
	var shards [4][]byte
	for i := range shards {
		shards[i] = []byte{byte(i)}
	}

	// Track delete calls
	var deletedSrc string
	mover := &notifyMover{
		shards:    shards[:],
		onDelete:  func(peer string) { deletedSrc = peer },
	}

	// The raft node auto-calls NotifyCommit on Propose (simulates immediate FSM apply)
	var exec *MigrationExecutor
	node := &notifyLoopRaft{onPropose: func() {
		exec.NotifyCommit("bucket", "key", "v1")
	}}

	exec = NewMigrationExecutor(mover, node, len(shards))

	task := MigrationTask{
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		SrcNode:   "src-node",
		DstNode:   "dst-node",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := exec.Execute(ctx, task)
	require.NoError(t, err)
	assert.Equal(t, "src-node", deletedSrc, "src shards should be deleted after commit")
}

// notifyMover is a ShardMover that reads from a fixed in-memory shard slice
// and tracks delete calls.
type notifyMover struct {
	shards   [][]byte
	written  [][]byte
	onDelete func(peer string)
}

func (m *notifyMover) ReadShard(_ context.Context, _ string, _, _ string, idx int) ([]byte, error) {
	return m.shards[idx], nil
}

func (m *notifyMover) WriteShard(_ context.Context, _ string, _, _ string, idx int, data []byte) error {
	if m.written == nil {
		m.written = make([][]byte, len(m.shards))
	}
	m.written[idx] = data
	return nil
}

func (m *notifyMover) DeleteShards(_ context.Context, peer, _, _ string) error {
	if m.onDelete != nil {
		m.onDelete(peer)
	}
	return nil
}

// notifyLoopRaft implements MigrationRaft and calls onPropose after each Propose.
type notifyLoopRaft struct {
	onPropose func()
}

func (r *notifyLoopRaft) Propose(data []byte) error {
	if r.onPropose != nil {
		r.onPropose()
	}
	return nil
}

func (r *notifyLoopRaft) NodeID() string { return "test-node" }

// TestBalancerIntegration_DiskCollector verifies that DiskCollector drives
// UpdateDiskStats → GossipSender skip guard → BalancerProposer triggers migration
// when a real DiskCollector with injected 80% disk usage runs alongside a peer at 20%.
func TestBalancerIntegration_DiskCollector(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)

	// Seed both nodes so GossipSender skip guard passes for local node.
	store.Set(NodeStats{NodeID: "leader", DiskUsedPct: 0.0})
	store.Set(NodeStats{NodeID: "peer-a", DiskUsedPct: 20.0, DiskAvailBytes: 200 << 30})

	node := &mockRaftNode{
		state:   2,
		nodeID:  "leader",
		peerIDs: []string{"peer-a"},
	}

	cfg := BalancerConfig{
		GossipInterval:      10 * time.Millisecond,
		WarmupTimeout:       1 * time.Millisecond,
		ImbalanceTriggerPct: 20.0,
		ImbalanceStopPct:    5.0,
		MigrationRate:       1,
		LeaderTenureMin:     0,
		LeaderLoadThreshold: 1.3,
	}

	p := NewBalancerProposer("leader", store, node, cfg)
	p.SetObjectPicker(&mockObjectPicker{bucket: "b", key: "k", ok: true})

	// Real DiskCollector with injected 80% disk usage.
	collector := NewDiskCollector("leader", "/tmp", store, 10*time.Millisecond)
	collector.SetStatFunc(func(string) (float64, uint64) { return 80.0, 1 << 30 })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go collector.Run(ctx)
	go p.Run(ctx)

	require.Eventually(t, func() bool {
		return node.ProposedLen() > 0
	}, 1*time.Second, 10*time.Millisecond, "timeout: no CmdMigrateShard proposed within 1s")

	var cmd clusterpb.Command
	require.NoError(t, proto.Unmarshal(node.ProposedAt(0), &cmd))
	assert.Equal(t, uint32(CmdMigrateShard), cmd.Type)
}
