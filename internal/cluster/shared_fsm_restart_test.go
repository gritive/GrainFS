package cluster

// shared_fsm_restart_test.go — Task 12: restart persistence + crash-recovery tests.
//
// TestSharedFSM_RestartPersistence — on-disk DB close+reopen; every group's
//   state survives and stays scoped.
// TestSharedFSM_RestoreCrashMidway_SelfHealsOnBoot — crash after DropPrefix
//   (using restoreCrashAfterDrop hook + panic/recover); reboot re-runs Restore
//   from durable snapshot and group self-heals; sibling group unaffected.
// TestSharedFSM_RestoreRejectsCorruptBytesBeforeDrop — garbled snapshot bytes
//   fail decode before any DropPrefix; pre-existing state intact.

import (
	"context"
	"path/filepath"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/raft"
)

// TestSharedFSM_RestartPersistence opens an on-disk shared FSM DB, writes state
// for two groups, closes the DB, reopens it, and asserts that every key survives
// with correct scoping.
func TestSharedFSM_RestartPersistence(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "shared-fsm")

	db, err := badger.Open(badgerutil.SmallOptions(dir))
	require.NoError(t, err)

	ksA := mustNewKS(t, "group-A")
	ksB := mustNewKS(t, "group-B")

	fA := NewFSM(db, ksA)
	fB := NewFSM(db, ksB)

	// Write objects and buckets into each group.
	putObjViaApply(t, fA, "bA", "obj1", "A-payload")
	putObjViaApply(t, fA, "bA", "obj2", "A-payload2")
	putObjViaApply(t, fB, "bB", "obj1", "B-payload")

	require.NoError(t, db.Close())

	// Reopen the same on-disk directory.
	db2, err := badger.Open(badgerutil.SmallOptions(dir))
	require.NoError(t, err)
	defer db2.Close()

	// All keys must still exist after reopen.
	assert.True(t, dbHasKey(t, db2, ksA.ObjectMetaKey("bA", "obj1")), "A obj1 must survive restart")
	assert.True(t, dbHasKey(t, db2, ksA.ObjectMetaKey("bA", "obj2")), "A obj2 must survive restart")
	assert.True(t, dbHasKey(t, db2, ksA.BucketKey("bA")), "A bucket must survive restart")
	assert.True(t, dbHasKey(t, db2, ksB.ObjectMetaKey("bB", "obj1")), "B obj1 must survive restart")
	assert.True(t, dbHasKey(t, db2, ksB.BucketKey("bB")), "B bucket must survive restart")

	// Values must be distinct — A and B had different payloads for obj1.
	var valA, valB []byte
	require.NoError(t, db2.View(func(txn *badger.Txn) error {
		item, err := txn.Get(ksA.ObjectMetaKey("bA", "obj1"))
		if err != nil {
			return err
		}
		valA, err = item.ValueCopy(nil)
		return err
	}))
	require.NoError(t, db2.View(func(txn *badger.Txn) error {
		item, err := txn.Get(ksB.ObjectMetaKey("bB", "obj1"))
		if err != nil {
			return err
		}
		valB, err = item.ValueCopy(nil)
		return err
	}))
	assert.NotEqual(t, valA, valB, "A-obj1 and B-obj1 values must be distinct after restart")

	// Backend-level scoping: A's backend sees only A's objects; B's backend
	// knows nothing about A's bucket.
	nodeA, _ := newTestNodeForSharedDB(t, "restart-nodeA")
	backendA, err := NewDistributedBackend(t.TempDir(), db2, nodeA, ksA, true)
	require.NoError(t, err)
	stopA := make(chan struct{})
	go backendA.RunApplyLoop(stopA)
	t.Cleanup(func() { close(stopA) })

	nodeB, _ := newTestNodeForSharedDB(t, "restart-nodeB")
	backendB, err := NewDistributedBackend(t.TempDir(), db2, nodeB, ksB, true)
	require.NoError(t, err)
	stopB := make(chan struct{})
	go backendB.RunApplyLoop(stopB)
	t.Cleanup(func() { close(stopB) })

	ctx := context.Background()

	// A's backend sees obj1 + obj2 in bA.
	objsA, err := backendA.ListObjects(ctx, "bA", "", 100)
	require.NoError(t, err)
	keysA := make([]string, 0, len(objsA))
	for _, o := range objsA {
		keysA = append(keysA, o.Key)
	}
	assert.ElementsMatch(t, []string{"obj1", "obj2"}, keysA, "A backend must see obj1+obj2 after restart")

	// B's backend must not see bA at all (bA belongs to group A).
	// ListObjects returns ErrBucketNotFound when the bucket doesn't exist in
	// the group's keyspace — that error IS the correct isolation proof.
	objsB, err := backendB.ListObjects(ctx, "bA", "", 100)
	if err != nil {
		// ErrBucketNotFound from group B's perspective is the expected isolation outcome.
		assert.Empty(t, objsB, "B backend must see nothing for bucket bA (belongs to group A)")
	} else {
		assert.Empty(t, objsB, "B backend must see nothing for bucket bA (belongs to group A)")
	}
}

// TestSharedFSM_RestoreCrashMidway_SelfHealsOnBoot proves that if the process
// is killed mid-Restore (after DropPrefix, before the re-write), the next reboot
// calls Restore again from the durable snapshot and the group self-heals. The
// sibling group is never touched across the crash or reboot.
//
// Implementation: uses the restoreCrashAfterDrop package-level hook (nil in
// production) and panic/recover to simulate the kill without a subprocess.
// Uses direct fA2.Restore on reboot (Eng-review-approved equivalent of
// SnapshotManager.Restore — same code path). Uses dummy-blob snapshot bytes
// (FSM.Restore does not validate value contents, only keys).
func TestSharedFSM_RestoreCrashMidway_SelfHealsOnBoot(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "shared-fsm")

	db, err := badger.Open(badgerutil.SmallOptions(dir))
	require.NoError(t, err)

	ksA := mustNewKS(t, "group-A")
	ksB := mustNewKS(t, "group-B")
	fA := NewFSM(db, ksA)
	fB := NewFSM(db, ksB)

	// Step 2: write group A's initial state and group B's state.
	putObjViaApply(t, fA, "bA", "old-obj", "OLD")
	putObjViaApply(t, fB, "bB", "keep-obj", "KEEP-B")

	// Step 3: build the incoming snapshot for group A — new desired state.
	// Use dummy-blob route: FSM.Restore does not validate value contents.
	snapData, err := marshalSnapshotState(map[string][]byte{
		"obj:bA/new-obj": []byte("dummy-blob"),
	})
	require.NoError(t, err)

	// Step 4: simulate the crash via panic/recover.
	// Set hook; ALWAYS reset it via defer so subsequent tests are not affected.
	restoreCrashAfterDrop = func() { panic("simulated crash mid-Restore") }
	defer func() { restoreCrashAfterDrop = nil }()

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic from restoreCrashAfterDrop hook, got none")
			}
		}()
		_ = fA.Restore(raft.SnapshotMeta{FormatVersion: raft.FSMSnapshotFormatVersion}, snapData)
	}()

	// Disable hook before any further Restore calls (reboot path).
	restoreCrashAfterDrop = nil

	// After the crash: group A's old-obj was DropPrefix'd; new-obj was never written.
	assert.False(t, dbHasKey(t, db, ksA.ObjectMetaKey("bA", "old-obj")),
		"old-obj must be gone: DropPrefix ran before the crash")
	assert.False(t, dbHasKey(t, db, ksA.ObjectMetaKey("bA", "new-obj")),
		"new-obj must not exist: re-write never ran (crash interrupted it)")

	// Sibling group B is intact through the crash.
	assert.True(t, dbHasKey(t, db, ksB.ObjectMetaKey("bB", "keep-obj")),
		"B's keep-obj must survive group-A's crash")

	// Step 5: simulate the reboot — close DB and reopen.
	require.NoError(t, db.Close())

	db2, err := badger.Open(badgerutil.SmallOptions(dir))
	require.NoError(t, err)
	defer db2.Close()

	fA2 := NewFSM(db2, ksA)
	fB2 := NewFSM(db2, ksB)
	_ = fB2 // created to mirror boot: both groups' FSMs are instantiated on startup

	// Re-run Restore from the durable snapshot (the boot path).
	// DropPrefix(ksA) is idempotent on the already-empty A prefix.
	require.NoError(t, fA2.Restore(raft.SnapshotMeta{FormatVersion: raft.FSMSnapshotFormatVersion}, snapData),
		"reboot Restore must succeed")

	// Group A self-healed: new-obj is now present.
	assert.True(t, dbHasKey(t, db2, ksA.ObjectMetaKey("bA", "new-obj")),
		"new-obj must be restored after reboot Restore")

	// Group B still intact: keep-obj survived both the crash and the reboot.
	assert.True(t, dbHasKey(t, db2, ksB.ObjectMetaKey("bB", "keep-obj")),
		"B's keep-obj must be intact after crash + reboot")
}

// TestSharedFSM_RestoreRejectsCorruptBytesBeforeDrop asserts that a garbled
// snapshot payload causes Restore to return an error during the decode phase,
// BEFORE any DropPrefix runs — leaving the group's pre-existing state intact.
//
// Coverage note: apply_test.go:TestFSM_Restore_CorruptData covers the empty
// keyspace (dropAllKeys path) and does not assert state preservation. This test
// covers the shared-keyspace (DropPrefix path) with the decode-before-drop
// invariant explicitly asserted.
func TestSharedFSM_RestoreRejectsCorruptBytesBeforeDrop(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "shared-fsm")
	db, err := badger.Open(badgerutil.SmallOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	ksA := mustNewKS(t, "group-A")
	fA := NewFSM(db, ksA)

	putObjViaApply(t, fA, "bA", "x", "X")

	// Garbled bytes — not a valid snapshot FlatBuffer.
	err = fA.Restore(raft.SnapshotMeta{FormatVersion: raft.FSMSnapshotFormatVersion},
		[]byte("not a valid snapshot blob \xff\xff\xff"))
	require.Error(t, err, "corrupt snapshot bytes must be rejected")

	// Pre-existing state must be intact — decode failed before DropPrefix.
	assert.True(t, dbHasKey(t, db, ksA.ObjectMetaKey("bA", "x")),
		"pre-existing key must survive a corrupt-snapshot rejection (decode-before-drop)")
}
