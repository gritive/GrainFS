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

// TestSharedFSM_BackendListObjects_ScopedToGroup was removed: object metadata is
// no longer written to or read from the shared FSM BadgerDB (it is blob-resident
// under blob-primary), so the cross-group object-collision risk this probed via
// HeadObject point reads is structurally gone. The FSM-keyspace isolation that
// remains relevant is covered by the snapshot/restore prefix-isolation tests
// (TestSharedFSM_SnapshotContainsOnlyOwnGroup / TestSharedFSM_RestoreReplacesOnlyOwnGroup)
// and the keyspace-distinctness rows in shared_fsm_isolation_test.go.

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
