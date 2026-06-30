package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// syncRecorder records the ORDER of fsync calls (file vs dir) via the
// ShardService test seams. The hooks are no-ops on a tmpfs — we assert the
// CALL SEQUENCE, since real power-loss is untestable in a unit test.
type syncRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *syncRecorder) file(*os.File) error {
	r.mu.Lock()
	r.events = append(r.events, "file")
	r.mu.Unlock()
	return nil
}

func (r *syncRecorder) dir(string) error {
	r.mu.Lock()
	r.events = append(r.events, "dir")
	r.mu.Unlock()
	return nil
}

func (r *syncRecorder) seq() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.events))
	copy(out, r.events)
	return out
}

// dirCount returns how many dir-fsync events were recorded.
func (r *syncRecorder) dirCount() int {
	n := 0
	for _, e := range r.seq() {
		if e == "dir" {
			n++
		}
	}
	return n
}

// TestWriteEncryptedShardFile_LockedFsyncOrder proves the D2 locked durability
// order on a class that requires fsync (large no-redundancy, requireFsync=true):
// the shard file is fsynced FIRST, THEN the parent-directory chain is fsynced
// (one or more dir events: leaf shard dir + each newly-created ancestor up to
// the data dir), all before the write returns.
//
// RED before S2: no syncDir call exists → seq == ["file"] (no dir events).
// Mutation: drop the s.syncDirChain(...) call → dirCount == 0 → RED.
func TestWriteEncryptedShardFile_LockedFsyncOrder(t *testing.T) {
	backend, _, _, _ := newS1ShardSvc(t,
		ECConfig{DataShards: 1, ParityShards: 0}, []string{"self"},
		WithNoRedundancy(func() bool { return true }))
	rec := &syncRecorder{}
	backend.shardSvc.local.syncFileHook = rec.file
	backend.shardSvc.local.syncDirHook = rec.dir

	large := bytes.Repeat([]byte("s2-locked-order-"), 1<<17) // > 1MiB, requireFsync=true
	_, err := backend.PutObject(context.Background(), "b", "obj", bytes.NewReader(large), "application/octet-stream")
	require.NoError(t, err)

	seq := rec.seq()
	require.NotEmpty(t, seq)
	require.Equal(t, "file", seq[0], "shard file must be fsynced FIRST")
	require.GreaterOrEqual(t, rec.dirCount(), 1, "at least the leaf shard dir must be fsynced after the file")
	for _, e := range seq[1:] {
		require.Equal(t, "dir", e, "every fsync after the file fsync must be a dir fsync (locked order)")
	}
}

// TestSmallShard_Fsynced proves S2: a small shard (< 1 MiB) is fsynced (file +
// dir chain) at write time.
func TestSmallShard_Fsynced(t *testing.T) {
	backend, _, _, _ := newS1ShardSvc(t,
		ECConfig{DataShards: 1, ParityShards: 0}, []string{"self"},
		WithNoRedundancy(func() bool { return true }))
	rec := &syncRecorder{}
	backend.shardSvc.local.syncFileHook = rec.file
	backend.shardSvc.local.syncDirHook = rec.dir

	small := []byte("tiny-s2") // << 1 MiB
	_, err := backend.PutObject(context.Background(), "b", "obj-small", bytes.NewReader(small), "application/octet-stream")
	require.NoError(t, err)

	require.NotEmpty(t, rec.seq())
	require.Equal(t, "file", rec.seq()[0], "small shard: file fsync first")
	require.GreaterOrEqual(t, rec.dirCount(), 1, "small shard: dir chain fsynced after the file")

	rc, _, err := backend.GetObject(context.Background(), "b", "obj-small")
	require.NoError(t, err)
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	require.Equal(t, small, got)
}

// TestLargeNoRedundancy_Fsynced proves the no-redundancy large path is fsynced
// (file + dir chain) directly because there is no EC parity to reconstruct from.
func TestLargeNoRedundancy_Fsynced(t *testing.T) {
	backend, _, _, _ := newS1ShardSvc(t,
		ECConfig{DataShards: 1, ParityShards: 0}, []string{"self"},
		WithNoRedundancy(func() bool { return true }))
	rec := &syncRecorder{}
	backend.shardSvc.local.syncFileHook = rec.file
	backend.shardSvc.local.syncDirHook = rec.dir

	large := bytes.Repeat([]byte("s2-large-noredund-"), 1<<17)
	_, err := backend.PutObject(context.Background(), "b", "obj-nr", bytes.NewReader(large), "application/octet-stream")
	require.NoError(t, err)

	require.NotEmpty(t, rec.seq())
	require.Equal(t, "file", rec.seq()[0], "no-redundancy large: file fsync first")
	require.GreaterOrEqual(t, rec.dirCount(), 1, "no-redundancy large: dir chain fsynced after the file")
}

// TestLargeRedundant_NoFsync proves the S1 class is untouched: large + EC
// redundancy is NOT fsynced (EC owns durability).
func TestLargeRedundant_NoFsync(t *testing.T) {
	backend, _, _, _ := newS1ShardSvc(t,
		ECConfig{DataShards: 2, ParityShards: 1}, []string{"self", "self", "self"})
	rec := &syncRecorder{}
	backend.shardSvc.local.syncFileHook = rec.file
	backend.shardSvc.local.syncDirHook = rec.dir

	large := bytes.Repeat([]byte("s2-large-redundant-"), 1<<17)
	_, err := backend.PutObject(context.Background(), "b", "obj-r", bytes.NewReader(large), "application/octet-stream")
	require.NoError(t, err)

	require.Empty(t, rec.seq(),
		"large redundant shards must NOT be fsynced — EC owns durability")
}

// TestDBV_NoRedundancyPath_DirFsyncFailureBlocksVisibility proves DBV on the
// no-redundancy path (1+0 → writeStreamShards → writeQuorumMeta; the single-local
// fast path was removed, so 1+0 now routes through the stream writer): a
// dir-fsync failure fails the PUT and the object never becomes visible.
func TestDBV_NoRedundancyPath_DirFsyncFailureBlocksVisibility(t *testing.T) {
	backend, _, _, _ := newS1ShardSvc(t,
		ECConfig{DataShards: 1, ParityShards: 0}, []string{"self"},
		WithNoRedundancy(func() bool { return true }))
	var dirCalls atomic.Int32
	backend.shardSvc.local.syncDirHook = func(string) error {
		dirCalls.Add(1)
		return errors.New("injected dir fsync failure")
	}

	large := bytes.Repeat([]byte("s2-dbv-single-"), 1<<17)
	_, err := backend.PutObject(context.Background(), "b", "obj-dbv1", bytes.NewReader(large), "application/octet-stream")
	require.Error(t, err, "a dir-fsync failure must fail the no-redundancy PUT (DBV)")
	require.ErrorContains(t, err, "injected dir fsync failure",
		"the PUT must fail FROM the durability hook, not from setup/placement")
	require.Positive(t, dirCalls.Load(), "the injected dir-fsync hook must have actually run")

	_, _, gerr := backend.GetObject(context.Background(), "b", "obj-dbv1")
	require.Error(t, gerr, "a PUT whose durability failed must not be visible via GET")
}

// TestDBV_ECCommitPath_DirFsyncFailureBlocksVisibility proves DBV on the EC
// commit path: 2+0 is multi-shard (writeDataShards → commitECObjectWriteResult →
// writeQuorumMeta) AND no-redundancy (requireFsync=true). 2 data shards need TWO
// placements — []string{"self","self"} — or planObjectWritePlacement rejects the
// PUT BEFORE any shard write (the ErrorContains + dirCalls guards catch that).
func TestDBV_ECCommitPath_DirFsyncFailureBlocksVisibility(t *testing.T) {
	backend, _, _, _ := newS1ShardSvc(t,
		ECConfig{DataShards: 2, ParityShards: 0}, []string{"self", "self"},
		WithNoRedundancy(func() bool { return true }))
	var dirCalls atomic.Int32
	backend.shardSvc.local.syncDirHook = func(string) error {
		dirCalls.Add(1)
		return errors.New("injected dir fsync failure")
	}

	large := bytes.Repeat([]byte("s2-dbv-ec-"), 1<<18) // splits across 2 data shards
	_, err := backend.PutObject(context.Background(), "b", "obj-dbv2", bytes.NewReader(large), "application/octet-stream")
	require.Error(t, err, "a dir-fsync failure must fail the EC-commit-path PUT (DBV)")
	require.ErrorContains(t, err, "injected dir fsync failure",
		"the PUT must fail FROM the durability hook on the EC commit path, not from setup/placement")
	require.Positive(t, dirCalls.Load(), "the injected dir-fsync hook must have actually run on a data shard")

	_, _, gerr := backend.GetObject(context.Background(), "b", "obj-dbv2")
	require.Error(t, gerr, "a PUT whose durability failed must not be visible via GET")
}
