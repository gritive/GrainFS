package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// newTestNodeForSharedDB spins up a single-node raft cluster backed by a temp
// directory, waits until it becomes leader, and registers cleanup. Unlike
// newTestDistributedBackend, the BadgerDB is NOT opened here — it is supplied
// by the caller so multiple backends can share the same DB.
func newTestNodeForSharedDB(t *testing.T, nodeID string) (node *raft.Node, logStore raft.LogStore) {
	t.Helper()
	dir := t.TempDir()
	var err error
	logStore, err = raft.NewBadgerLogStore(dir + "/raft")
	require.NoError(t, err)

	cfg := raft.DefaultConfig(nodeID, nil)
	node = raft.NewNode(cfg, logStore)
	node.SetTransport(
		func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
			return nil, fmt.Errorf("no peers")
		},
		func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
			return nil, fmt.Errorf("no peers")
		},
	)
	node.Start()
	for range 200 {
		if node.State() == raft.Leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.Equal(t, raft.Leader, node.State(), "test node %s must become leader", nodeID)
	t.Cleanup(func() {
		node.Stop()
		logStore.Close()
	})
	return node, logStore
}

// TestSharedFSM_BackendListObjects_ScopedToGroup verifies that two
// DistributedBackends sharing one BadgerDB but using distinct stateKeyspaces
// never see each other's objects. It drives writes via FSM.Apply (no Raft
// round-trip needed) and reads via the real ListObjects + HeadObject iterator
// paths.
func TestSharedFSM_BackendListObjects_ScopedToGroup(t *testing.T) {
	// Shared in-memory BadgerDB.
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	ksA, err := newStateKeyspace("group-A")
	require.NoError(t, err)
	ksB, err := newStateKeyspace("group-B")
	require.NoError(t, err)

	fA := NewFSM(db, ksA)
	fB := NewFSM(db, ksB)

	// Helper: write a bucket + object into a group's FSM (same bucket name,
	// same object key — to prove there is no cross-group collision).
	putObj := func(t *testing.T, f *FSM, bucket, key, etag string) {
		t.Helper()
		raw, err := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: bucket})
		require.NoError(t, err)
		_ = f.Apply(raw) // idempotent: ignore ErrBucketAlreadyExists (applied twice is fine)

		raw, err = EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
			Bucket:      bucket,
			Key:         key,
			Size:        int64(len(etag)),
			ContentType: "text/plain",
			ETag:        etag,
			ModTime:     1,
		})
		require.NoError(t, err)
		require.NoError(t, f.Apply(raw))
	}

	const bucket = "shared-bucket"
	putObj(t, fA, bucket, "obj1", "A-payload")
	putObj(t, fA, bucket, "obj2-only-in-A", "A2")
	putObj(t, fB, bucket, "obj1", "B-payload")

	// Two DistributedBackends over the same DB with distinct keyspaces, shared=true.
	// Each backend gets its own raft node so it can process proposals.
	rootA := t.TempDir()
	nodeA, _ := newTestNodeForSharedDB(t, "node-A")
	backendA, err := NewDistributedBackend(rootA, db, nodeA, ksA, true)
	require.NoError(t, err)
	stopA := make(chan struct{})
	go backendA.RunApplyLoop(stopA)
	t.Cleanup(func() { close(stopA) })

	rootB := t.TempDir()
	nodeB, _ := newTestNodeForSharedDB(t, "node-B")
	backendB, err := NewDistributedBackend(rootB, db, nodeB, ksB, true)
	require.NoError(t, err)
	stopB := make(chan struct{})
	go backendB.RunApplyLoop(stopB)
	t.Cleanup(func() { close(stopB) })

	ctx := context.Background()

	// --- backendA: must see obj1 + obj2-only-in-A ---
	objsA, err := backendA.ListObjects(ctx, bucket, "", 100)
	require.NoError(t, err)
	keysA := make([]string, 0, len(objsA))
	for _, o := range objsA {
		keysA = append(keysA, o.Key)
	}
	assert.ElementsMatch(t, []string{"obj1", "obj2-only-in-A"}, keysA,
		"group-A ListObjects should return exactly obj1 and obj2-only-in-A")

	// --- backendB: must see only obj1 ---
	objsB, err := backendB.ListObjects(ctx, bucket, "", 100)
	require.NoError(t, err)
	keysB := make([]string, 0, len(objsB))
	for _, o := range objsB {
		keysB = append(keysB, o.Key)
	}
	assert.ElementsMatch(t, []string{"obj1"}, keysB,
		"group-B ListObjects should return exactly obj1")

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
