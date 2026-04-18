package cluster

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockShardMover records calls to ReadShard, WriteShard, and DeleteShards.
type mockShardMover struct {
	mu        sync.Mutex
	writeErr  error
	deleteErr error
	writes    []shardWriteCall
	deletes   []shardDeleteCall
}

type shardWriteCall struct {
	peer, bucket, key string
	shardIdx          int
}

type shardDeleteCall struct {
	peer, bucket, key string
}

func (m *mockShardMover) WriteShard(_ context.Context, peer, bucket, key string, shardIdx int, _ []byte) error {
	m.mu.Lock()
	m.writes = append(m.writes, shardWriteCall{peer, bucket, key, shardIdx})
	m.mu.Unlock()
	return m.writeErr
}

func (m *mockShardMover) ReadShard(_ context.Context, _, _, _ string, _ int) ([]byte, error) {
	return []byte("shard-data"), nil
}

func (m *mockShardMover) DeleteShards(_ context.Context, peer, bucket, key string) error {
	m.mu.Lock()
	m.deletes = append(m.deletes, shardDeleteCall{peer, bucket, key})
	m.mu.Unlock()
	return m.deleteErr
}

// mockMigrationRaft captures Propose calls and immediately notifies the executor.
type mockMigrationRaft struct {
	mu       sync.Mutex
	proposed [][]byte
	nodeID   string
	exec     *MigrationExecutor // set after executor is created
}

func (m *mockMigrationRaft) Propose(data []byte) error {
	m.mu.Lock()
	m.proposed = append(m.proposed, data)
	exec := m.exec
	m.mu.Unlock()
	// Simulate immediate Raft commit: notify the executor
	if exec != nil {
		exec.NotifyCommit("b", "k", "v1")
	}
	return nil
}

func (m *mockMigrationRaft) NodeID() string { return m.nodeID }

// --- MigrationExecutor tests ---

func TestMigrationExecutor_CopiesThenProposeDone(t *testing.T) {
	mover := &mockShardMover{}
	node := &mockMigrationRaft{nodeID: "node-a"}
	exec := NewMigrationExecutor(mover, node, 6)
	node.exec = exec // wire up for immediate commit simulation

	err := exec.Execute(context.Background(), MigrationTask{
		Bucket: "b", Key: "k", VersionID: "v1", SrcNode: "node-b", DstNode: "node-c",
	})
	require.NoError(t, err)

	// All 6 shards should be written to dst
	assert.Len(t, mover.writes, 6)
	for _, w := range mover.writes {
		assert.Equal(t, "node-c", w.peer)
		assert.Equal(t, "b", w.bucket)
	}

	// CmdMigrationDone should be proposed exactly once
	require.Len(t, node.proposed, 1)

	// Src should be deleted after commit
	require.Len(t, mover.deletes, 1)
	assert.Equal(t, "node-b", mover.deletes[0].peer)
}

func TestMigrationExecutor_AbortOnWriteFailure(t *testing.T) {
	mover := &mockShardMover{writeErr: errors.New("network error")}
	node := &mockMigrationRaft{nodeID: "node-a"}
	exec := NewMigrationExecutor(mover, node, 6)
	node.exec = exec

	err := exec.Execute(context.Background(), MigrationTask{
		Bucket: "b", Key: "k", VersionID: "v1", SrcNode: "node-b", DstNode: "node-c",
	})
	require.Error(t, err)

	// No proposal and no delete when write fails
	assert.Empty(t, node.proposed)
	assert.Empty(t, mover.deletes)
}

func TestMigrationExecutor_NoDeleteIfCommitTimeout(t *testing.T) {
	mover := &mockShardMover{}
	// node that never calls NotifyCommit
	node := &mockMigrationRaft{nodeID: "node-a"}
	exec := NewMigrationExecutor(mover, node, 6)
	// don't wire node.exec — commit will never arrive

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := exec.Execute(ctx, MigrationTask{
		Bucket: "b", Key: "k", VersionID: "v1", SrcNode: "node-b", DstNode: "node-c",
	})
	require.Error(t, err, "should fail if commit times out")

	// Src must NOT be deleted if commit was not confirmed
	assert.Empty(t, mover.deletes, "must not delete src when commit not confirmed")
}

func TestMigrationExecutor_IdempotentByVersionID(t *testing.T) {
	mover := &mockShardMover{}
	node := &mockMigrationRaft{nodeID: "node-a"}
	exec := NewMigrationExecutor(mover, node, 6)
	node.exec = exec

	task := MigrationTask{Bucket: "b", Key: "k", VersionID: "v1", SrcNode: "node-b", DstNode: "node-c"}

	require.NoError(t, exec.Execute(context.Background(), task))

	// Reset mock state
	mover.mu.Lock()
	mover.writes = nil
	mover.deletes = nil
	mover.mu.Unlock()
	node.mu.Lock()
	node.proposed = nil
	node.mu.Unlock()

	// Second execution of same task should be skipped
	require.NoError(t, exec.Execute(context.Background(), task))
	assert.Empty(t, mover.writes, "second execution of same migration should be skipped")
}

func TestMigrationExecutor_NotifyCommit_Unblocks(t *testing.T) {
	mover := &mockShardMover{}
	node := &mockMigrationRaft{nodeID: "node-a"}
	exec := NewMigrationExecutor(mover, node, 6)

	task := MigrationTask{Bucket: "b", Key: "k", VersionID: "v1", SrcNode: "node-b", DstNode: "node-c"}

	done := make(chan error, 1)
	go func() {
		done <- exec.Execute(context.Background(), task)
	}()

	// Give Execute() time to reach WaitForCommit
	time.Sleep(20 * time.Millisecond)

	// Manually trigger commit notification
	exec.NotifyCommit("b", "k", "v1")

	select {
	case err := <-done:
		require.NoError(t, err)
		require.Len(t, mover.deletes, 1)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Execute did not unblock after NotifyCommit")
	}
}
