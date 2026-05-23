package cluster

import (
	"reflect"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// applyBatched applies raw commands to fsm, grouping them into transactions of
// the given sizes, and returns the per-entry error strings ("" for nil).
func applyBatched(t *testing.T, fsm *FSM, cmds [][]byte, batchSizes []int) []string {
	t.Helper()
	results := make([]string, 0, len(cmds))
	i := 0
	for _, n := range batchSizes {
		txn := fsm.db.NewTransaction(true)
		for j := 0; j < n && i < len(cmds); j++ {
			if err := fsm.ApplyTxn(txn, cmds[i]); err != nil {
				results = append(results, err.Error())
			} else {
				results = append(results, "")
			}
			i++
		}
		require.NoError(t, txn.Commit())
	}
	require.Equal(t, len(cmds), i, "batchSizes must cover every command")
	return results
}

// dumpFSMState returns every key/value in fsm's DB as a map for comparison.
func dumpFSMState(t *testing.T, fsm *FSM) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := fsm.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
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

// determinismCmdSequence builds a fixed sequence exercising read-modify-write
// handlers. The PutObjectMeta(v1) -> PutObjectMeta(v2) -> DeleteObjectVersion(v2)
// triplet on the same object exercises the iterator read-your-writes path:
// when v2 (the latest) is deleted, applyDeleteObjectVersion's scanGroupPrefix
// must see v1 — pending in the same shared transaction when these three land
// in one batch.
func determinismCmdSequence(t *testing.T) [][]byte {
	t.Helper()
	enc := func(ct CommandType, p any) []byte {
		b, err := EncodeCommand(ct, p)
		require.NoError(t, err)
		return b
	}
	return [][]byte{
		enc(CmdCreateBucket, CreateBucketCmd{Bucket: "b1"}),
		enc(CmdPutObjectMeta, PutObjectMetaCmd{Bucket: "b1", Key: "k1", Size: 10, ETag: "e1", VersionID: "v1"}),
		enc(CmdPutObjectMeta, PutObjectMetaCmd{Bucket: "b1", Key: "k1", Size: 20, ETag: "e2", VersionID: "v2"}),
		enc(CmdDeleteObjectVersion, DeleteObjectVersionCmd{Bucket: "b1", Key: "k1", VersionID: "v2"}),
		enc(CmdCreateBucket, CreateBucketCmd{Bucket: "b2"}),
		enc(CmdSetBucketVersioning, SetBucketVersioningCmd{Bucket: "b2", State: "Enabled"}),
		enc(CmdPutObjectMeta, PutObjectMetaCmd{Bucket: "b2", Key: "k2", Size: 5, ETag: "e3", VersionID: "v3"}),
		enc(CmdDeleteObject, DeleteObjectCmd{Bucket: "b2", Key: "k2", VersionID: "v4"}),
	}
}

func TestApplyTxnBatchDeterminism(t *testing.T) {
	cmds := determinismCmdSequence(t)
	n := len(cmds)

	// Batch groupings. Each must sum to n. nil entry == one transaction per cmd.
	groupings := [][]int{
		nil,        // unbatched (reference)
		{2, 2, 2, 2},
		{n},        // single batch — exercises iterator pending writes
		{1, 3, 1, 3},
	}

	var refState map[string]string
	var refResults []string
	for gi, g := range groupings {
		if g == nil {
			g = make([]int, n)
			for i := range g {
				g[i] = 1
			}
		}
		fsm := NewFSM(newTestDB(t), newStateKeyspaceEmpty())
		results := applyBatched(t, fsm, cmds, g)
		state := dumpFSMState(t, fsm)
		if gi == 0 {
			refState, refResults = state, results
			continue
		}
		require.True(t, reflect.DeepEqual(state, refState),
			"grouping %d: DB state diverged from unbatched", gi)
		require.Equal(t, refResults, results,
			"grouping %d: result vector diverged", gi)
	}
}

func TestApplyBatch_CommitFailureFallback(t *testing.T) {
	cmds := determinismCmdSequence(t)
	batch := make([]raft.LogEntry, len(cmds))
	for i, c := range cmds {
		batch[i] = raft.LogEntry{Index: uint64(i + 1), Term: 1, Type: raft.LogEntryCommand, Command: c}
	}

	// Reference: unbatched apply on a separate FSM.
	refFSM := NewFSM(newTestDB(t), newStateKeyspaceEmpty())
	for _, c := range cmds {
		_ = refFSM.Apply(c)
	}
	refState := dumpFSMState(t, refFSM)

	// Force the batch commit to fail once; subsequent commits go through.
	orig := commitApplyTxn
	failed := false
	commitApplyTxn = func(txn *badger.Txn) error {
		if !failed {
			failed = true
			txn.Discard()
			return badger.ErrConflict
		}
		return orig(txn)
	}
	defer func() { commitApplyTxn = orig }()

	fsm := NewFSM(newTestDB(t), newStateKeyspaceEmpty())
	a := &applyActor{db: fsm.db, fsm: fsm}
	results := a.applyBatch(batch)

	require.True(t, failed, "commit-failure seam was not exercised")
	require.Len(t, results, len(cmds))
	require.Equal(t, refState, dumpFSMState(t, fsm),
		"fallback must produce the same state as unbatched apply")
}

func TestApplyBatch_BusinessErrorDoesNotAbortBatch(t *testing.T) {
	enc := func(ct CommandType, p any) []byte {
		b, err := EncodeCommand(ct, p)
		require.NoError(t, err)
		return b
	}
	cmds := [][]byte{
		enc(CmdCreateBucket, CreateBucketCmd{Bucket: "b1"}),
		enc(CmdPutObjectMeta, PutObjectMetaCmd{Bucket: "b1", Key: "k1", Size: 1, ETag: "e1"}),
		// CAS against a wrong ETag -> business error, no write.
		enc(CmdPutObjectMeta, PutObjectMetaCmd{Bucket: "b1", Key: "k1", Size: 2, ETag: "e2", ExpectedETag: "WRONG"}),
		enc(CmdPutObjectMeta, PutObjectMetaCmd{Bucket: "b1", Key: "k3", Size: 3, ETag: "e3"}),
	}
	batch := make([]raft.LogEntry, len(cmds))
	for i, c := range cmds {
		batch[i] = raft.LogEntry{Index: uint64(i + 1), Term: 1, Type: raft.LogEntryCommand, Command: c}
	}

	fsm := NewFSM(newTestDB(t), newStateKeyspaceEmpty())
	a := &applyActor{db: fsm.db, fsm: fsm}
	results := a.applyBatch(batch)

	require.NoError(t, results[0])
	require.NoError(t, results[1])
	require.Error(t, results[2], "CAS mismatch must be reported")
	require.NoError(t, results[3])

	// Entry 3 (k3) committed despite entry 2's error.
	err := fsm.db.View(func(txn *badger.Txn) error {
		_, e := txn.Get(fsm.keys.ObjectMetaKey("b1", "k3"))
		return e
	})
	require.NoError(t, err, "entries after a business error must still commit")
}

func TestApplyBatch_ErrTxnTooBigFallback(t *testing.T) {
	// Many large PutObjectMeta commands whose summed writes exceed the Badger
	// txn limit force a mid-batch ErrTxnTooBig. The fallback must re-apply each
	// entry individually so no committed Raft entry is silently dropped.
	bigMeta := map[string]string{"x": strings.Repeat("a", 64<<10)} // 64 KiB each
	var cmds [][]byte
	for i := 0; i < 32; i++ {
		b, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
			Bucket: "b1", Key: "k" + string(rune('a'+i)), Size: 1, ETag: "e",
			UserMetadata: bigMeta,
		})
		require.NoError(t, err)
		cmds = append(cmds, b)
	}
	batch := make([]raft.LogEntry, len(cmds))
	for i, c := range cmds {
		batch[i] = raft.LogEntry{Index: uint64(i + 1), Term: 1, Type: raft.LogEntryCommand, Command: c}
	}

	refFSM := NewFSM(newTestDB(t), newStateKeyspaceEmpty())
	for _, c := range cmds {
		require.NoError(t, refFSM.Apply(c))
	}

	fsm := NewFSM(newTestDB(t), newStateKeyspaceEmpty())
	a := &applyActor{db: fsm.db, fsm: fsm}
	results := a.applyBatch(batch)

	for i, r := range results {
		require.NoError(t, r, "entry %d must be applied via fallback, not dropped", i)
	}
	require.Equal(t, dumpFSMState(t, refFSM), dumpFSMState(t, fsm),
		"every entry past the overflow point must be applied")
}
