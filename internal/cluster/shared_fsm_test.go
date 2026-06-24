package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgermeta"
	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/raft"
)

// newTestNodeForSharedDB spins up a single-node raft cluster backed by a temp
// directory, waits until it becomes leader, and registers cleanup. Unlike
// newTestDistributedBackend, the BadgerDB is NOT opened here — it is supplied
// by the caller so multiple backends can share the same DB.
func newTestNodeForSharedDB(t *testing.T, nodeID string) (node RaftNode, closeFn func() error) {
	t.Helper()
	dir := t.TempDir()
	cfg := raft.DefaultConfig(nodeID, nil)
	node, closeFn, err := newRaftNode(cfg, dir)
	require.NoError(t, err)
	node.SetTransport(
		func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
			return nil, fmt.Errorf("no peers")
		},
		func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
			return nil, fmt.Errorf("no peers")
		},
	)
	node.Start()
	require.NoError(t, node.Bootstrap())
	for range 200 {
		if node.IsLeader() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.True(t, node.IsLeader(), "test node %s must become leader", nodeID)
	t.Cleanup(func() {
		node.Close()
		if closeFn != nil {
			_ = closeFn()
		}
	})
	return node, closeFn
}

// TestSharedFSM_BackendListObjects_ScopedToGroup verifies that two
// DistributedBackends sharing one BadgerDB but using distinct stateKeyspaces
// never see each other's objects. Writes via FSM.Apply land in BadgerDB only;
// Phase 4 LIST uses quorum meta (shardSvc), so group isolation is verified
// via HeadObject point reads (BadgerDB path) rather than ListObjects.
func TestSharedFSM_BackendListObjects_ScopedToGroup(t *testing.T) {
	// Shared in-memory BadgerDB.
	db, err := badger.Open(badgerutil.SmallOptions("").WithInMemory(true))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	ksA, err := newStateKeyspace("group-A")
	require.NoError(t, err)
	ksB, err := newStateKeyspace("group-B")
	require.NoError(t, err)

	fA := NewFSM(badgermeta.Wrap(db), ksA)
	fB := NewFSM(badgermeta.Wrap(db), ksB)

	// Helper: write an object into a group's FSM (same bucket name, same object
	// key — to prove there is no cross-group collision). CmdPutObjectMeta is a
	// no-op in the FSM after Slice 2; write via persistPutObjectMetaUpdate
	// directly. Bucket existence is supplied separately via each backend's
	// MetaBucketStore (the sole authority since Task 12).
	putObj := func(t *testing.T, f *FSM, bucket, key, etag string) {
		t.Helper()
		cmd := PutObjectMetaCmd{
			Bucket: bucket, Key: key, Size: int64(len(etag)), ContentType: "text/plain", ETag: etag, ModTime: 1,
		}
		require.NoError(t, f.db.Update(func(txn MetadataTxn) error {
			return f.persistPutObjectMetaUpdate(txn, cmd, buildPutObjectMeta(cmd))
		}))
	}

	const bucket = "shared-bucket"
	putObj(t, fA, bucket, "obj1", "A-payload")
	putObj(t, fA, bucket, "obj2-only-in-A", "A2")
	putObj(t, fB, bucket, "obj1", "B-payload")

	// Two DistributedBackends over the same DB with distinct keyspaces, shared=true.
	// Each backend gets its own raft node so it can process proposals.
	rootA := t.TempDir()
	nodeA, _ := newTestNodeForSharedDB(t, "node-A")
	backendA, err := NewDistributedBackend(rootA, badgermeta.Wrap(db), nodeA, ksA, true)
	require.NoError(t, err)
	stopA := make(chan struct{})
	go backendA.RunApplyLoop(stopA)
	t.Cleanup(func() { close(stopA) })

	rootB := t.TempDir()
	nodeB, _ := newTestNodeForSharedDB(t, "node-B")
	backendB, err := NewDistributedBackend(rootB, badgermeta.Wrap(db), nodeB, ksB, true)
	require.NoError(t, err)
	stopB := make(chan struct{})
	go backendB.RunApplyLoop(stopB)
	t.Cleanup(func() { close(stopB) })

	ctx := context.Background()

	// HeadBucket reads MetaBucketStore (sole authority since Task 12). Register the
	// shared bucket name in each backend's own MBS so existence resolves per group.
	seedBucketsInMBS(t, backendA, bucket)
	seedBucketsInMBS(t, backendB, bucket)

	// Phase 4: LIST uses quorum meta (shardSvc path). Shared-FSM backends
	// have no shardSvc, so HeadObject point reads prove isolation instead.

	// --- Point read: group-A's obj1 has A-payload, NOT B-payload ---
	objA, _, err := backendA.headObjectMeta(ctx, bucket, "obj1")
	require.NoError(t, err)
	assert.Equal(t, "A-payload", objA.ETag, "group-A should see A-payload for obj1")

	// --- Point read: group-B's obj1 has B-payload ---
	objB, _, err := backendB.headObjectMeta(ctx, bucket, "obj1")
	require.NoError(t, err)
	assert.Equal(t, "B-payload", objB.ETag, "group-B should see B-payload for obj1")

	// --- group-B must not find obj2-only-in-A ---
	_, _, err = backendB.headObjectMeta(ctx, bucket, "obj2-only-in-A")
	assert.Error(t, err, "obj2-only-in-A should not be visible from group-B")
}

// putObjViaApply writes an object into a group's FSM. The object meta is written
// via persistPutObjectMetaUpdate directly because CmdPutObjectMeta is a no-op in
// the FSM after data-plane raft-free Slice 2. Bucket existence is no longer seeded
// here (CmdCreateBucket is a retired no-op in Task 12; HeadBucket reads the sole
// authority, MetaBucketStore) — callers that exercise HeadBucket must wire an MBS
// via seedBucketsInMBS.
func putObjViaApply(t *testing.T, f *FSM, bucket, key, etag string) {
	t.Helper()
	cmd := PutObjectMetaCmd{
		Bucket: bucket, Key: key, Size: int64(len(etag)), ContentType: "text/plain", ETag: etag, ModTime: 1,
	}
	require.NoError(t, f.db.Update(func(txn MetadataTxn) error {
		return f.persistPutObjectMetaUpdate(txn, cmd, buildPutObjectMeta(cmd))
	}))
}

// seedBucketsInMBS wires a fresh direct-FSM MetaBucketStore onto the backend and
// registers each named bucket, so HeadBucket (the sole-authority existence check
// since Task 12) resolves. Used by shared-FSM tests that build backends directly
// via NewDistributedBackend (which does not wire an MBS — serveruntime does that
// in production, newTestDistributedBackend in unit tests).
func seedBucketsInMBS(t *testing.T, b *DistributedBackend, buckets ...string) {
	t.Helper()
	mbs := newDirectFSMMetaBucketStore(b.fsm)
	for _, bucket := range buckets {
		require.NoError(t, mbs.CreateBucket(context.Background(), bucket, "local", false))
	}
	b.SetMetaBucketStore(mbs)
}

// fsmHasKey reports whether the group-relative key exists in f's keyspace.
func fsmHasKey(t *testing.T, f *FSM, rawKey string) bool {
	t.Helper()
	found := false
	require.NoError(t, f.db.View(func(txn MetadataTxn) error {
		_, err := txn.Get(f.keys.Key([]byte(rawKey)))
		if errors.Is(err, ErrMetaKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return nil
	}))
	return found
}

const v2Format = raft.FSMSnapshotFormatVersion

func TestSharedFSM_SnapshotContainsOnlyOwnGroup(t *testing.T) {
	db, err := badger.Open(badgerutil.SmallOptions("").WithInMemory(true))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	ksA, err := newStateKeyspace("group-A")
	require.NoError(t, err)
	ksB, err := newStateKeyspace("group-B")
	require.NoError(t, err)
	fA := NewFSM(badgermeta.Wrap(db), ksA)
	fB := NewFSM(badgermeta.Wrap(db), ksB)

	putObjViaApply(t, fA, "bucket", "objA", "A-pay")
	putObjViaApply(t, fB, "bucket", "objB", "B-pay")

	blob, err := fA.Snapshot()
	require.NoError(t, err)
	state, err := unmarshalSnapshotState(blob)
	require.NoError(t, err)

	for k := range state {
		assert.False(t, ksB.HasPrefix([]byte(k)), "snapshot key %q must not belong to group B", k)
		assert.False(t, ksA.HasPrefix([]byte(k)), "snapshot key %q must be group-RELATIVE (not prefixed)", k)
	}
	_, ok := state["obj:bucket/objA"]
	assert.True(t, ok, "fA's own object key must be present in group-relative form")
}

func TestSharedFSM_RestoreReplacesOnlyOwnGroup(t *testing.T) {
	db, err := badger.Open(badgerutil.SmallOptions("").WithInMemory(true))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	ksA, err := newStateKeyspace("group-A")
	require.NoError(t, err)
	ksB, err := newStateKeyspace("group-B")
	require.NoError(t, err)
	fA := NewFSM(badgermeta.Wrap(db), ksA)
	fB := NewFSM(badgermeta.Wrap(db), ksB)

	putObjViaApply(t, fA, "bucket", "old-A", "old")
	putObjViaApply(t, fB, "bucket", "keep-B", "keep")

	blob, err := marshalSnapshotState(map[string][]byte{
		"obj:bucket/new-A": []byte("new-A-payload"),
	})
	require.NoError(t, err)
	require.NoError(t, fA.Restore(raft.SnapshotMeta{FormatVersion: v2Format}, blob))

	assert.False(t, fsmHasKey(t, fA, "obj:bucket/old-A"), "old-A must be gone after restore")
	assert.True(t, fsmHasKey(t, fA, "obj:bucket/new-A"), "new-A must be present after restore")
	assert.True(t, fsmHasKey(t, fB, "obj:bucket/keep-B"), "sibling group B's key must survive restore of A")
}

func TestSharedFSM_RestoreRejectsWrongFormatVersion(t *testing.T) {
	db, err := badger.Open(badgerutil.SmallOptions("").WithInMemory(true))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	ksA, err := newStateKeyspace("group-A")
	require.NoError(t, err)
	fA := NewFSM(badgermeta.Wrap(db), ksA)
	putObjViaApply(t, fA, "bucket", "objA", "pay")

	blob, err := fA.Snapshot()
	require.NoError(t, err)

	err = fA.Restore(raft.SnapshotMeta{FormatVersion: 1}, blob)
	require.Error(t, err)
	assert.True(t, fsmHasKey(t, fA, "obj:bucket/objA"), "pre-existing state must be untouched on rejected restore")
}

func TestSharedFSM_RestoreRejectsAlreadyPrefixedKeys(t *testing.T) {
	db, err := badger.Open(badgerutil.SmallOptions("").WithInMemory(true))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	ksA, err := newStateKeyspace("group-A")
	require.NoError(t, err)
	fA := NewFSM(badgermeta.Wrap(db), ksA)
	putObjViaApply(t, fA, "bucket", "objA", "pay")

	blob, err := marshalSnapshotState(map[string][]byte{
		string(ksA.Key([]byte("obj:b/z"))): []byte("z"),
	})
	require.NoError(t, err)
	err = fA.Restore(raft.SnapshotMeta{FormatVersion: v2Format}, blob)
	require.Error(t, err)
	assert.True(t, fsmHasKey(t, fA, "obj:bucket/objA"), "pre-existing state must be untouched on rejected restore")
}

func TestFSM_Restore_EmptyKeyspace_WholeDBReplace(t *testing.T) {
	db, err := badger.Open(badgerutil.SmallOptions("").WithInMemory(true))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	f := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())
	putObjViaApply(t, f, "bucket", "old", "old")
	require.True(t, fsmHasKey(t, f, "obj:bucket/old"))

	blob, err := marshalSnapshotState(map[string][]byte{
		"obj:bucket/new": []byte("new-payload"),
	})
	require.NoError(t, err)
	require.NoError(t, f.Restore(raft.SnapshotMeta{FormatVersion: v2Format}, blob))

	assert.False(t, fsmHasKey(t, f, "obj:bucket/old"), "old key must be gone (whole-DB drop)")
	// Empty keyspace ⇒ raw key, no prefix added.
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte("obj:bucket/new"))
		return err
	}))
}
