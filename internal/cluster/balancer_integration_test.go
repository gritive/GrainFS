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

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go p.Run(ctx)

	// Wait for at least one proposal
	deadline := time.After(400 * time.Millisecond)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout: no CmdMigrateShard proposed within 400ms")
		case <-time.After(10 * time.Millisecond):
			if len(node.proposed) > 0 {
				goto done
			}
		}
	}
done:
	require.NotEmpty(t, node.proposed)
	var cmd clusterpb.Command
	require.NoError(t, proto.Unmarshal(node.proposed[0], &cmd))
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
