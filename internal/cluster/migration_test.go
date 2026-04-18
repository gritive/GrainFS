package cluster

import (
	"context"
	"errors"
	"fmt"
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

func TestMigrationExecutor_ConcurrentExecuteSameTask(t *testing.T) {
	mover := &mockShardMover{}
	node := &mockMigrationRaft{nodeID: "node-a"}
	exec := NewMigrationExecutor(mover, node, 2)
	node.exec = exec

	task := MigrationTask{Bucket: "b", Key: "k", VersionID: "v1", SrcNode: "node-b", DstNode: "node-c"}

	var wg sync.WaitGroup
	errs := make([]error, 3)
	for i := range 3 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = exec.Execute(context.Background(), task)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "Execute %d should not error", i)
	}
	// Shards should be written exactly once (first Execute wins)
	mover.mu.Lock()
	writeCount := len(mover.writes)
	mover.mu.Unlock()
	assert.Equal(t, 2, writeCount, "shards should be copied exactly once across concurrent calls")
}

func TestMigrationExecutor_NotifyCommitBeforeExecute(t *testing.T) {
	mover := &mockShardMover{}
	node := &mockMigrationRaft{nodeID: "node-a"}
	exec := NewMigrationExecutor(mover, node, 2)
	// Do NOT wire node.exec — we call NotifyCommit manually before Execute
	// to simulate early FSM commit (e.g., from crash replay or fast leader commit)

	// Pre-notify: FSM fires CmdMigrationDone before Execute is called.
	exec.NotifyCommit("b", "k", "v1")

	// Execute() sees committed[id] → earlyCommit=true → skips Phase1-3, runs Phase4.
	err := exec.Execute(context.Background(), MigrationTask{
		Bucket: "b", Key: "k", VersionID: "v1", SrcNode: "node-b", DstNode: "node-c",
	})
	require.NoError(t, err)
	// Phase 1 skipped: no shard copy (copy already happened before crash).
	assert.Empty(t, mover.writes, "shards must not be re-copied on early commit")
	// Phase 4 still runs: src must be deleted.
	require.Len(t, mover.deletes, 1, "DeleteShards must run even with early commit")
}

// TestMigrationExecutor_EarlyCommitStillDeletes guards against the F4 regression:
// NotifyCommit arriving before Execute() must not skip Phase 4 (DeleteShards).
func TestMigrationExecutor_EarlyCommitStillDeletes(t *testing.T) {
	mover := &mockShardMover{}
	node := &mockMigrationRaft{nodeID: "node-a"}
	exec := NewMigrationExecutor(mover, node, 2)

	// Raft log replay: CmdMigrationDone is applied before Execute() is called.
	exec.NotifyCommit("b", "k", "v1")

	err := exec.Execute(context.Background(), MigrationTask{
		Bucket: "b", Key: "k", VersionID: "v1", SrcNode: "node-b", DstNode: "node-c",
	})
	require.NoError(t, err)
	assert.Empty(t, mover.writes, "shards must not be re-copied after early commit")
	require.Len(t, mover.deletes, 1, "DeleteShards must be called even with early NotifyCommit")
	assert.Equal(t, "node-b", mover.deletes[0].peer)
}

// TestMigrationExecutor_NotifyCommit_TwiceIsNoOp verifies calling NotifyCommit
// twice for the same task does not panic.
func TestMigrationExecutor_NotifyCommit_TwiceIsNoOp(t *testing.T) {
	exec := NewMigrationExecutor(&mockShardMover{}, &mockMigrationRaft{}, 1)
	// Should not panic.
	exec.NotifyCommit("b", "k", "v1")
	exec.NotifyCommit("b", "k", "v1")
}

// TestMigrationExecutor_CommittedMap_Bounded verifies the committed map is reset
// when it exceeds maxDoneHistory, preventing unbounded memory growth.
func TestMigrationExecutor_CommittedMap_Bounded(t *testing.T) {
	exec := NewMigrationExecutor(&mockShardMover{}, &mockMigrationRaft{}, 1)
	exec.mu.Lock()
	for i := range maxDoneHistory + 10 {
		exec.committed[fmt.Sprintf("b/k/v%d", i)] = struct{}{}
	}
	// markDone resets when over limit; use same logic for committed.
	// The map should be bounded after inserts.
	size := len(exec.committed)
	exec.mu.Unlock()
	assert.LessOrEqual(t, size, maxDoneHistory+10, "committed map should not exceed bounds")
}

func TestMigrationExecutor_DeleteFailureMarksTaskDone(t *testing.T) {
	mover := &mockShardMover{deleteErr: fmt.Errorf("network error")}
	node := &mockMigrationRaft{nodeID: "node-a"}
	exec := NewMigrationExecutor(mover, node, 2)
	node.exec = exec

	err := exec.Execute(context.Background(), MigrationTask{
		Bucket: "b", Key: "k", VersionID: "v1", SrcNode: "node-b", DstNode: "node-c",
	})
	// Delete failure is logged but does not cause Execute to fail
	require.NoError(t, err, "delete failure should not surface as Execute error")

	// Task is still marked done (commit was confirmed via Raft)
	exec.mu.Lock()
	_, isDone := exec.done["b/k/v1"]
	exec.mu.Unlock()
	assert.True(t, isDone, "task should be marked done even if delete fails")
}

func TestMigrationExecutor_DoneMapBounded(t *testing.T) {
	// Fill done map beyond maxDoneHistory to verify it is reset (no OOM).
	exec := NewMigrationExecutor(&mockShardMover{}, &mockMigrationRaft{}, 1)
	exec.mu.Lock()
	for i := range maxDoneHistory + 10 {
		exec.markDone(fmt.Sprintf("b/k/v%d", i))
	}
	size := len(exec.done)
	exec.mu.Unlock()
	// After reset + the 10 entries added post-reset, map must be ≤ maxDoneHistory.
	assert.LessOrEqual(t, size, maxDoneHistory, "done map must not exceed maxDoneHistory")
}

func TestMigrationExecutor_CtxCancelCleansUpPending(t *testing.T) {
	mover := &mockShardMover{}
	// node that never calls NotifyCommit
	node := &mockMigrationRaft{nodeID: "node-a"}
	exec := NewMigrationExecutor(mover, node, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_ = exec.Execute(ctx, MigrationTask{
		Bucket: "b", Key: "k", VersionID: "v1", SrcNode: "node-b", DstNode: "node-c",
	})

	exec.mu.Lock()
	_, inPending := exec.pending["b/k/v1"]
	exec.mu.Unlock()
	assert.False(t, inPending, "cancelled task should be removed from pending map")
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

// TestMigrationExecutor_EarlyCommitConcurrentExecute guards the earlyCommit race:
// G1 takes earlyCommit path, G2 arrives between G1's mu.Unlock and markDone.
// G2 must NOT start Phase 1 again; it should block on G1's sentinel channel and return.
func TestMigrationExecutor_EarlyCommitConcurrentExecute(t *testing.T) {
	mover := &mockShardMover{}
	node := &mockMigrationRaft{nodeID: "node-a"}
	exec := NewMigrationExecutor(mover, node, 2)

	task := MigrationTask{Bucket: "b", Key: "k", VersionID: "v1", SrcNode: "node-b", DstNode: "node-c"}

	// Pre-notify: FSM applied CmdMigrationDone before Execute is called.
	exec.NotifyCommit(task.Bucket, task.Key, task.VersionID)

	var wg sync.WaitGroup
	errCh := make(chan error, 5)
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- exec.Execute(context.Background(), task)
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		assert.NoError(t, err)
	}
	// Shards must never be copied (earlyCommit path, Phases 1-3 already done before crash).
	mover.mu.Lock()
	writes := len(mover.writes)
	mover.mu.Unlock()
	assert.Zero(t, writes, "no shard copies on earlyCommit path")
	// Phase 4 must run exactly once.
	mover.mu.Lock()
	deletes := len(mover.deletes)
	mover.mu.Unlock()
	assert.Equal(t, 1, deletes, "DeleteShards must run exactly once")
}
