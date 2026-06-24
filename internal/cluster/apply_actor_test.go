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

// determinismCmdSequence builds a fixed sequence of retired-slot commands to
// verify batch-apply determinism across different transaction groupings.
//
// Task 12: CmdCreateBucket/CmdDeleteBucket/CmdSetBucketVersioning/
// CmdSetBucketPolicy/CmdDeleteBucketPolicy are all retired (group-0 bucket
// control-plane moved to meta-raft). Their applies are no-ops; the sequence
// exercises the batch-actor infrastructure (not the applies themselves) by
// confirming the same final DB state is produced regardless of how commands
// are grouped into transactions.
func determinismCmdSequence(t *testing.T) [][]byte {
	t.Helper()
	enc := func(ct CommandType, p any) []byte {
		b, err := EncodeCommand(ct, p)
		require.NoError(t, err)
		return b
	}
	return [][]byte{
		enc(CmdCreateBucket, CreateBucketCmd{Bucket: "b1"}),
		enc(CmdSetBucketVersioning, SetBucketVersioningCmd{Bucket: "b1", State: "Enabled"}),
		enc(CmdSetBucketPolicy, SetBucketPolicyCmd{Bucket: "b1", PolicyJSON: []byte(`{"v":1}`)}),
		enc(CmdDeleteBucketPolicy, DeleteBucketPolicyCmd{Bucket: "b1"}),
		enc(CmdCreateBucket, CreateBucketCmd{Bucket: "b2"}),
		enc(CmdSetBucketVersioning, SetBucketVersioningCmd{Bucket: "b2", State: "Enabled"}),
		enc(CmdSetBucketPolicy, SetBucketPolicyCmd{Bucket: "b2", PolicyJSON: []byte(`{"v":2}`)}),
		enc(CmdDeleteBucket, DeleteBucketCmd{Bucket: "b2"}),
	}
}

func TestApplyTxnBatchDeterminism(t *testing.T) {
	cmds := determinismCmdSequence(t)
	n := len(cmds)

	// Batch groupings. Each must sum to n. nil entry == one transaction per cmd.
	groupings := [][]int{
		nil, // unbatched (reference)
		{2, 2, 2, 2},
		{n}, // single batch — exercises iterator pending writes
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
		fsm := NewFSM(newTestStore(t), newStateKeyspaceEmpty())
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
	refFSM := NewFSM(newTestStore(t), newStateKeyspaceEmpty())
	for _, c := range cmds {
		_ = refFSM.Apply(c)
	}
	refState := dumpFSMState(t, refFSM)

	// Force the batch commit to fail once; subsequent commits go through.
	orig := commitApplyTxn
	failed := false
	commitApplyTxn = func(txn MetadataTxn) error {
		if !failed {
			failed = true
			txn.Discard()
			return badger.ErrConflict
		}
		return orig(txn)
	}
	defer func() { commitApplyTxn = orig }()

	fsm := NewFSM(newTestStore(t), newStateKeyspaceEmpty())
	a := &applyActor{db: fsm.db, fsm: fsm}
	results := a.applyBatch(batch)

	require.True(t, failed, "commit-failure seam was not exercised")
	require.Len(t, results, len(cmds))
	require.Equal(t, refState, dumpFSMState(t, fsm),
		"fallback must produce the same state as unbatched apply")
}

func TestApplyBatch_BusinessErrorDoesNotAbortBatch(t *testing.T) {
	// Build a batch where entry 2 (index 2) has a corrupt payload for a
	// live command (CmdResealFSMValues decode will fail on garbage bytes).
	// Entries before and after must be applied successfully.
	//
	// Note: the bucket commands (CmdCreateBucket, CmdDeleteBucket) were retired
	// in Task 12 (group-0 bucket control-plane → meta-raft); their applies are
	// no-ops that cannot produce a business error. We use CmdResealFSMValues
	// with a garbage payload to get a decode error at entry 2.
	goodNoOp, err := buildRawCommand(CmdNoOp, nil)
	require.NoError(t, err)
	// Empty payload triggers "empty data" error in fbSafe, which is a reliable
	// decode error independent of FlatBuffers internals.
	badReseal, err := buildRawCommand(CmdResealFSMValues, nil)
	require.NoError(t, err)

	cmds := [][]byte{
		goodNoOp,  // entry 0: no-op → nil
		goodNoOp,  // entry 1: no-op → nil
		badReseal, // entry 2: decode error → error
		goodNoOp,  // entry 3: no-op → nil (must not be aborted)
	}
	batch := make([]raft.LogEntry, len(cmds))
	for i, c := range cmds {
		batch[i] = raft.LogEntry{Index: uint64(i + 1), Term: 1, Type: raft.LogEntryCommand, Command: c}
	}

	fsm := NewFSM(newTestStore(t), newStateKeyspaceEmpty())
	a := &applyActor{db: fsm.db, fsm: fsm}
	results := a.applyBatch(batch)

	require.NoError(t, results[0])
	require.NoError(t, results[1])
	require.Error(t, results[2], "corrupt CmdResealFSMValues payload must be reported as error")
	require.NoError(t, results[3], "entries after a business error must still be applied")
}

func TestApplyBatch_ErrTxnTooBigFallback(t *testing.T) {
	// Many large PutObjectMeta commands whose summed writes exceed the Badger
	// txn limit force a mid-batch ErrTxnTooBig. The fallback must re-apply each
	// entry individually so no committed Raft entry is silently dropped.
	bigMeta := map[string]string{"x": strings.Repeat("a", 64<<10)} // 64 KiB each
	var cmds [][]byte
	for i := 0; i < 32; i++ {
		// Retired object slot (CommandType 3, formerly CmdPutObjectMeta): the apply
		// loop no-ops it but must still process every committed entry without dropping.
		payload, err := encodeQuorumMetaBlob(PutObjectMetaCmd{
			Bucket: "b1", Key: "k" + string(rune('a'+i)), Size: 1, ETag: "e",
			UserMetadata: bigMeta,
		})
		require.NoError(t, err)
		b, err := buildRawCommand(CommandType(3), payload)
		require.NoError(t, err)
		cmds = append(cmds, b)
	}
	batch := make([]raft.LogEntry, len(cmds))
	for i, c := range cmds {
		batch[i] = raft.LogEntry{Index: uint64(i + 1), Term: 1, Type: raft.LogEntryCommand, Command: c}
	}

	refFSM := NewFSM(newTestStore(t), newStateKeyspaceEmpty())
	for _, c := range cmds {
		require.NoError(t, refFSM.Apply(c))
	}

	fsm := NewFSM(newTestStore(t), newStateKeyspaceEmpty())
	a := &applyActor{db: fsm.db, fsm: fsm}
	results := a.applyBatch(batch)

	for i, r := range results {
		require.NoError(t, r, "entry %d must be applied via fallback, not dropped", i)
	}
	require.Equal(t, dumpFSMState(t, refFSM), dumpFSMState(t, fsm),
		"every entry past the overflow point must be applied")
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

	// Task 12: CmdCreateBucket is retired (group-0 bucket control-plane moved to
	// meta-raft); its apply is a no-op and writes no keys. Use
	// persistPutObjectMetaUpdate to write real state into the source FSM so the
	// snapshot has actual content to restore, and use ObjectMetaKey for assertions.
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
	// Commands use retired CmdNoOp (wire-stable) since the batch actor must handle
	// all committed log entries, including no-ops and retired slots.
	noOp := func() []byte {
		raw, nerr := buildRawCommand(CmdNoOp, nil)
		require.NoError(t, nerr)
		return raw
	}

	fsm := NewFSM(newTestStore(t), newStateKeyspaceEmpty())
	b := &DistributedBackend{store: fsm.db, fsm: fsm, node: snapshotBarrierFakeNode{}, registry: NewRegistry()}
	a := &applyActor{db: fsm.db, fsm: fsm}

	// Write pre-snapshot objects directly (pre1, pre2) — simulates state that
	// the snapshot should wipe.
	writeObj(fsm, "snap-bkt", "pre1", "pre-etag")
	writeObj(fsm, "snap-bkt", "pre2", "pre-etag")

	// The post-snapshot command (c3) MUST mutate observable FSM state so the test
	// fails if its apply were silently dropped (lastApplied alone is not enough —
	// commitBatch bumps it for every collected entry regardless of apply effect).
	// Use CmdMigrateShard with an unbuffered, receiver-less migration channel: the
	// non-blocking send falls through to the persist branch, which writes a
	// PendingMigrationKey — a distinguishable, deterministic post-snapshot mutation.
	fsm.SetMigrationHooks(make(chan MigrationTask), nil, nil)
	c3Migrate := MigrateShardFSMCmd{
		Bucket: "snap-bkt", Key: "post-snap", VersionID: "v1",
		SrcNode: "node-a", DstNode: "node-b",
	}
	c3, err := EncodeCommand(CmdMigrateShard, c3Migrate)
	require.NoError(t, err)
	postSnapKey := string(fsm.keys.PendingMigrationKey(c3Migrate.Bucket, c3Migrate.Key, c3Migrate.VersionID))

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
	// The post-snapshot command's apply MUST have mutated state: its
	// PendingMigrationKey must be present. This fails if c3's apply were dropped.
	require.Contains(t, state, postSnapKey,
		"post-snapshot command must be applied and mutate FSM state (not silently dropped)")
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
