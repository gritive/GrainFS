package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMigrationExecutor_StopIdempotent verifies that calling Stop() twice
// does not panic (sync.Once prevents double-close of quit channel).
func TestMigrationExecutor_StopIdempotent(t *testing.T) {
	mover := &mockShardMover{}
	node := &mockMigrationRaft{nodeID: "node-a"}
	e := NewMigrationExecutor(mover, node, 1)

	require.NotPanics(t, func() {
		e.Stop()
		e.Stop()
	})
}

// --- fakeMover / fakeRaft helpers for per-object shard-count tests ---

// fakeMover controls which shard indices exist and records which were written.
type fakeMover struct {
	shards      map[int][]byte
	movedShards []int
}

func (f *fakeMover) ReadShard(_ context.Context, _, _, _ string, shardIdx int) ([]byte, error) {
	d, ok := f.shards[shardIdx]
	if !ok {
		return nil, fmt.Errorf("shard %d not found", shardIdx)
	}
	return d, nil
}

func (f *fakeMover) WriteShard(_ context.Context, _, _, _ string, shardIdx int, _ []byte) error {
	f.movedShards = append(f.movedShards, shardIdx)
	return nil
}

func (f *fakeMover) DeleteShards(_ context.Context, _, _, _ string) error { return nil }

// fakeRaft simulates immediate Raft commit by calling exec.NotifyCommit after Propose.
type fakeRaft struct {
	exec *MigrationExecutor
}

func (f *fakeRaft) Propose(data []byte) error {
	cmd, err := DecodeCommand(data)
	if err != nil {
		return err
	}
	done, derr := decodeMigrationDoneCmd(cmd.Data)
	if derr == nil && f.exec != nil {
		f.exec.NotifyCommit(done.Bucket, done.Key, done.VersionID)
	}
	return nil
}

func (f *fakeRaft) NodeID() string { return "node-0" }

// TestMigrationExecutor_NxMode_OnlyShardZero: without SetShardCounter, reading
// shardIdx=1 from a N× object (only shard 0 exists) must return an error.
func TestMigrationExecutor_NxMode_OnlyShardZero(t *testing.T) {
	t.Parallel()
	mover := &fakeMover{shards: map[int][]byte{0: []byte("data")}}
	exec := NewMigrationExecutor(mover, &fakeRaft{}, 3)
	err := exec.Execute(context.Background(), MigrationTask{
		Bucket: "bkt", Key: "obj", VersionID: "v1",
	})
	require.Error(t, err, "N× 모드에서 numShards=3이면 shardIdx=1,2 읽기 실패해야 함")
}

// TestMigrationExecutor_NxMode_WithShardCounter_Succeeds: SetShardCounter=1 restricts
// migration to shardIdx=0 only — Execute must succeed and touch exactly one shard.
func TestMigrationExecutor_NxMode_WithShardCounter_Succeeds(t *testing.T) {
	t.Parallel()
	mover := &fakeMover{shards: map[int][]byte{0: []byte("data")}}
	fr := &fakeRaft{}
	exec := NewMigrationExecutor(mover, fr, 3)
	fr.exec = exec
	exec.SetShardCounter(func(_, _, _ string) int { return 1 })
	err := exec.Execute(context.Background(), MigrationTask{
		Bucket: "bkt", Key: "obj", VersionID: "v1",
	})
	require.NoError(t, err, "shardCountFor=1이면 shardIdx=0만 이동, 성공해야 함")
	assert.Equal(t, []int{0}, mover.movedShards)
}

// TestMigrationExecutor_ECMode_WithShardCounter_AllShards: SetShardCounter=3 moves
// all three shards (k=2 data + m=1 parity) for an EC object.
func TestMigrationExecutor_ECMode_WithShardCounter_AllShards(t *testing.T) {
	t.Parallel()
	mover := &fakeMover{shards: map[int][]byte{0: []byte("d0"), 1: []byte("d1"), 2: []byte("p0")}}
	fr := &fakeRaft{}
	exec := NewMigrationExecutor(mover, fr, 3)
	fr.exec = exec
	exec.SetShardCounter(func(_, _, _ string) int { return 3 })
	err := exec.Execute(context.Background(), MigrationTask{
		Bucket: "bkt", Key: "obj", VersionID: "v1",
	})
	require.NoError(t, err)
	assert.Equal(t, []int{0, 1, 2}, mover.movedShards)
}

// TestMigrationExecutor_ShardCounter_ZeroFallback: SetShardCounter returning 0 must
// fall back to numShards (not loop zero times, skipping migration entirely).
func TestMigrationExecutor_ShardCounter_ZeroFallback(t *testing.T) {
	t.Parallel()
	mover := &fakeMover{shards: map[int][]byte{0: []byte("data"), 1: []byte("d1"), 2: []byte("p0")}}
	fr := &fakeRaft{}
	exec := NewMigrationExecutor(mover, fr, 3)
	fr.exec = exec
	exec.SetShardCounter(func(_, _, _ string) int { return 0 }) // 0 → fallback to numShards=3
	err := exec.Execute(context.Background(), MigrationTask{
		Bucket: "bkt", Key: "obj", VersionID: "v1",
	})
	require.NoError(t, err)
	assert.Equal(t, []int{0, 1, 2}, mover.movedShards, "numShards=3 fallback: all 3 shards must be moved")
}

// TestMigrationExecutor_SweepLoopExitsOnStop verifies that Start() launches the
// sweep goroutine and Stop() terminates it via the quit channel.
func TestMigrationExecutor_SweepLoopExitsOnStop(t *testing.T) {
	mover := &mockShardMover{}
	node := &mockMigrationRaft{nodeID: "node-a"}
	e := NewMigrationExecutorWithTTL(mover, node, 1, 100*time.Millisecond)

	ctx := context.Background()
	e.Start(ctx)

	done := make(chan struct{})
	go func() {
		e.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not return within 2s — sweepLoop likely goroutine-leaked")
	}
}
