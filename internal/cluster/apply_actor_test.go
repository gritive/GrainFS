package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// dumpFSMState returns every key/value in fsm's DB as a map for comparison.
func dumpFSMState(t *testing.T, fsm *FSM) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := fsm.db.View(func(txn MetadataTxn) error {
		it := txn.NewIterator(MetaIteratorOptions{PrefetchValues: true})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			out[string(item.KeyCopy(nil))] = string(v)
		}
		return nil
	})
	require.NoError(t, err)
	return out
}

func TestCommitBatch_CommandEntriesAdvanceCursorWithoutDecode(t *testing.T) {
	cmds := [][]byte{
		[]byte("legacy command bytes are opaque now"),
		nil,
	}
	batch := make([]raft.LogEntry, len(cmds))
	for i, c := range cmds {
		batch[i] = raft.LogEntry{Index: uint64(i + 1), Term: 7, Type: raft.LogEntryCommand, Command: c}
	}

	fsm := NewFSM(newTestStore(t), newStateKeyspaceEmpty())
	a := &applyActor{fsm: fsm}
	b := &DistributedBackend{fsm: fsm}
	a.batch = append(a.batch[:0], batch...)
	a.commitBatch(b)

	require.Equal(t, uint64(2), b.lastApplied.Load())
	require.Equal(t, uint64(7), b.lastAppliedTerm.Load())
	require.Empty(t, dumpFSMState(t, fsm), "retired data-group command entries must not mutate the FSM DB")
}

// snapshotBarrierFakeNode is a RaftNode stub for the snapshot-barrier test. It
// embeds RaftNode so unimplemented methods panic on the nil interface; only the
// methods the test path exercises (Configuration) need real implementations.
type snapshotBarrierFakeNode struct {
	RaftNode
}

func (snapshotBarrierFakeNode) Configuration() raft.Configuration {
	return raft.Configuration{}
}

func TestApplyActor_SnapshotIsBatchBarrier(t *testing.T) {
	// This test exercises the drain loop's snapshot barrier; force batching on
	// regardless of GRAINFS_RAFT_APPLY_BATCH_MAX.
	origCap := applyBatchEntriesCap
	applyBatchEntriesCap = maxApplyBatchEntries
	defer func() { applyBatchEntriesCap = origCap }()

	// Write real state into the source FSM so the snapshot has actual content to
	// restore, and use ObjectMetaKey for assertions.
	writeObj := func(f *FSM, bucket, key, etag string) {
		t.Helper()
		cmd := PutObjectMetaCmd{
			Bucket: bucket, Key: key, Size: 1, ContentType: "text/plain", ETag: etag, ModTime: 1,
		}
		require.NoError(t, f.db.Update(func(txn MetadataTxn) error {
			return f.persistPutObjectMetaUpdate(txn, cmd, buildPutObjectMeta(cmd))
		}))
	}

	// Source FSM with one object → snapshot bytes.
	src := NewFSM(newTestStore(t), newStateKeyspaceEmpty())
	writeObj(src, "snap-bkt", "from-snap", "snap-etag")
	snapBytes, err := src.Snapshot()
	require.NoError(t, err)

	// Target FSM + backend. Pre-snapshot object keys are written via direct DB
	// update (simulating entries already applied before the snapshot arrives).
	// Feed: command(pre1), command(pre2), snapshot, command(post).
	// Commands are opaque: the batch actor must drain committed command entries
	// without decoding or mutating the FSM.
	noOp := func() []byte {
		return []byte("opaque cursor marker")
	}

	fsm := NewFSM(newTestStore(t), newStateKeyspaceEmpty())
	b := &DistributedBackend{store: fsm.db, fsm: fsm, node: snapshotBarrierFakeNode{}}
	a := &applyActor{fsm: fsm}

	// Write pre-snapshot objects directly (pre1, pre2) — simulates state that
	// the snapshot should wipe.
	writeObj(fsm, "snap-bkt", "pre1", "pre-etag")
	writeObj(fsm, "snap-bkt", "pre2", "pre-etag")

	// The post-snapshot command must replay as a no-op after the snapshot
	// without blocking or creating a pending-migration key.
	c3 := []byte("opaque post-snapshot marker")
	postSnapKey := string(fsm.keys.PendingMigrationKey("snap-bkt", "post-snap", "v1"))

	c1 := noOp()
	c2 := noOp()

	ch := make(chan raft.LogEntry, 8)
	ch <- raft.LogEntry{Index: 2, Term: 1, Type: raft.LogEntryCommand, Command: c2}
	ch <- raft.LogEntry{Index: 3, Term: 1, Type: raft.LogEntrySnapshot, Command: snapBytes}
	ch <- raft.LogEntry{Index: 4, Term: 1, Type: raft.LogEntryCommand, Command: c3}

	first := raft.LogEntry{Index: 1, Term: 1, Type: raft.LogEntryCommand, Command: c1}
	require.True(t, a.collect(b, first, ch))

	// Drain the post-snapshot command.
	e4 := <-ch
	require.True(t, a.collect(b, e4, ch))

	state := dumpFSMState(t, fsm)
	require.Contains(t, state, string(fsm.keys.ObjectMetaKey("snap-bkt", "from-snap")),
		"snapshot must restore from-snap object")
	require.NotContains(t, state, string(fsm.keys.ObjectMetaKey("snap-bkt", "pre1")),
		"pre-snapshot state must be wiped by Restore")
	require.NotContains(t, state, string(fsm.keys.ObjectMetaKey("snap-bkt", "pre2")),
		"pre-snapshot state must be wiped by Restore")
	require.NotContains(t, state, postSnapKey,
		"retired migration replay must not create pending-migration state")
	require.Equal(t, uint64(4), b.lastApplied.Load())
}

func TestApplyBatchMaxEnvOverride(t *testing.T) {
	require.Equal(t, 64, applyBatchMax(""))
	require.Equal(t, 1, applyBatchMax("1"))
	require.Equal(t, 1, applyBatchMax("0")) // 0 disables batching -> cap 1
	require.Equal(t, 8, applyBatchMax("8"))
	require.Equal(t, 64, applyBatchMax("garbage"))
	require.Equal(t, 64, applyBatchMax("999")) // clamped to maxApplyBatchEntries
}
