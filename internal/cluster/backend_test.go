package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
)

type blockingSnapshotter struct {
	entered chan struct{}
	release chan struct{}
	closed  atomic.Bool
}

func newBlockingSnapshotter() *blockingSnapshotter {
	return &blockingSnapshotter{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *blockingSnapshotter) Snapshot() ([]byte, error) {
	if s.closed.CompareAndSwap(false, true) {
		close(s.entered)
	}
	<-s.release
	return []byte("blocked-snapshot"), nil
}

func (s *blockingSnapshotter) Restore(raft.SnapshotMeta, []byte) error {
	return nil
}

// newTestDistributedBackend creates a DistributedBackend backed by a local Raft node.
func newTestDistributedBackend(t testing.TB) *DistributedBackend {
	t.Helper()
	dir := t.TempDir()

	metaDir := dir + "/meta"
	dbOpts := badger.DefaultOptions(metaDir).WithLogger(nil)
	db, err := badger.Open(dbOpts)
	require.NoError(t, err)

	cfg := raft.DefaultConfig("test-node", nil)
	node, closeRaft, err := newRaftNode(cfg, dir)
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
	require.True(t, node.IsLeader(), "no-peers node must become leader")

	backend, err := NewDistributedBackend(dir, db, node, nil, false)
	require.NoError(t, err)

	stopApply := make(chan struct{})
	go backend.RunApplyLoop(stopApply)

	t.Cleanup(func() {
		close(stopApply)
		node.Close()
		db.Close()
		if closeRaft != nil {
			_ = closeRaft()
		}
	})

	return backend
}

func TestDistributedBackend_CreateAndHeadBucket(t *testing.T) {
	b := newTestDistributedBackend(t)

	require.NoError(t, b.CreateBucket(context.Background(), "test"))
	require.NoError(t, b.HeadBucket(context.Background(), "test"))
	require.ErrorIs(t, b.HeadBucket(context.Background(), "nope"), storage.ErrBucketNotFound)
}

func TestDistributedBackend_CreateBucketConflict(t *testing.T) {
	b := newTestDistributedBackend(t)

	require.NoError(t, b.CreateBucket(context.Background(), "dup"))
	require.ErrorIs(t, b.CreateBucket(context.Background(), "dup"), storage.ErrBucketAlreadyExists)
}

func TestProposalForwardPeersFallsBackToShardServicePeers(t *testing.T) {
	got := proposalForwardPeers(nil, []string{"127.0.0.1:7001", "127.0.0.1:7002"}, "127.0.0.1:7002")
	require.Equal(t, []string{"127.0.0.1:7001"}, got)
}

func TestDistributedBackend_ListBuckets(t *testing.T) {
	b := newTestDistributedBackend(t)

	require.NoError(t, b.CreateBucket(context.Background(), "alpha"))
	require.NoError(t, b.CreateBucket(context.Background(), "beta"))

	buckets, err := b.ListBuckets(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"alpha", "beta"}, buckets)
}

func TestDistributedBackend_DeleteBucket(t *testing.T) {
	b := newTestDistributedBackend(t)

	require.NoError(t, b.CreateBucket(context.Background(), "del"))
	require.NoError(t, b.DeleteBucket(context.Background(), "del"))
	require.ErrorIs(t, b.HeadBucket(context.Background(), "del"), storage.ErrBucketNotFound)
}

func TestDistributedBackend_DeleteBucketNotFound(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.ErrorIs(t, b.DeleteBucket(context.Background(), "nope"), storage.ErrBucketNotFound)
}

func TestDistributedBackend_DeleteBucketNotEmpty(t *testing.T) {
	b := newTestDistributedBackend(t)

	require.NoError(t, b.CreateBucket(context.Background(), "notempty"))
	_, err := b.PutObject(context.Background(), "notempty", "file.txt", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)

	require.ErrorIs(t, b.DeleteBucket(context.Background(), "notempty"), storage.ErrBucketNotEmpty)
}

func TestDistributedBackend_PutAndGetObject(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	obj, err := b.PutObject(context.Background(), "bucket", "hello.txt", strings.NewReader("hello world"), "text/plain")
	require.NoError(t, err)
	require.Equal(t, int64(11), obj.Size)
	require.Equal(t, "text/plain", obj.ContentType)
	require.NotEmpty(t, obj.ETag)

	rc, gotObj, err := b.GetObject(context.Background(), "bucket", "hello.txt")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	require.Equal(t, "hello world", string(data))
	require.Equal(t, obj.ETag, gotObj.ETag)
	require.Equal(t, obj.Size, gotObj.Size)
}

func TestDistributedBackend_GetObjectRejectsLocalSizeMismatch(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	obj, err := b.PutObject(context.Background(), "bucket", "hello.txt", strings.NewReader("hello world"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, os.Truncate(b.objectPathV("bucket", "hello.txt", obj.VersionID), 0))

	rc, _, err := b.GetObject(context.Background(), "bucket", "hello.txt")
	require.Error(t, err)
	require.Nil(t, rc)
	require.Contains(t, err.Error(), "local object size mismatch")
}

func TestDistributedBackend_PutObjectToBadBucket(t *testing.T) {
	b := newTestDistributedBackend(t)

	_, err := b.PutObject(context.Background(), "nope", "file.txt", strings.NewReader("data"), "text/plain")
	require.ErrorIs(t, err, storage.ErrBucketNotFound)
}

// TestDistributedBackend_PutObject_NilShardSvc_WithPlacementCtx_TakesNxPath verifies
// that shardSvc==nil routes to the Nx (non-EC) path even when the context carries a
// PlacementGroupEntry (as injected by contextForForwardedGroup for forwarded requests).
func TestDistributedBackend_PutObject_NilShardSvc_WithPlacementCtx_TakesNxPath(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	// Inject placement entry to simulate a forwarded-request context.
	ctx := contextForForwardedGroup(context.Background(), NewDataGroup("group-1", []string{"n1", "n2"}))

	obj, err := b.PutObject(ctx, "bucket", "key.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err, "nil-shardSvc backend must succeed via Nx path even with placement context")
	require.Equal(t, "key.txt", obj.Key)
}

func TestDistributedBackend_HeadObject(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	_, err := b.PutObject(context.Background(), "bucket", "meta.txt", strings.NewReader("metadata"), "text/plain")
	require.NoError(t, err)

	obj, err := b.HeadObject(context.Background(), "bucket", "meta.txt")
	require.NoError(t, err)
	require.Equal(t, int64(8), obj.Size)
	require.Equal(t, "meta.txt", obj.Key)
}

func TestDistributedBackend_HeadObjectNotFound(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	_, err := b.HeadObject(context.Background(), "bucket", "nope.txt")
	require.ErrorIs(t, err, storage.ErrObjectNotFound)
}

func TestDistributedBackend_DeleteObject(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	_, err := b.PutObject(context.Background(), "bucket", "del.txt", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)

	require.NoError(t, b.DeleteObject(context.Background(), "bucket", "del.txt"))

	_, err = b.HeadObject(context.Background(), "bucket", "del.txt")
	require.ErrorIs(t, err, storage.ErrObjectNotFound)
}

func TestDistributedBackend_DeleteObjectBadBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.ErrorIs(t, b.DeleteObject(context.Background(), "nope", "file.txt"), storage.ErrBucketNotFound)
}

func TestDistributedBackend_ListObjects(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	for _, kv := range []struct{ key, val string }{
		{"docs/a.txt", "a"},
		{"docs/b.txt", "b"},
		{"images/c.png", "c"},
	} {
		_, err := b.PutObject(context.Background(), "bucket", kv.key, strings.NewReader(kv.val), "text/plain")
		require.NoError(t, err)
	}

	objects, err := b.ListObjects(context.Background(), "bucket", "", 100)
	require.NoError(t, err)
	require.Len(t, objects, 3)

	objects, err = b.ListObjects(context.Background(), "bucket", "docs/", 100)
	require.NoError(t, err)
	require.Len(t, objects, 2)
}

func TestDistributedBackend_ListObjectsMaxKeys(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	for i := range 5 {
		_, err := b.PutObject(context.Background(), "bucket", fmt.Sprintf("file%d.txt", i), strings.NewReader("x"), "text/plain")
		require.NoError(t, err)
	}

	objects, err := b.ListObjects(context.Background(), "bucket", "", 3)
	require.NoError(t, err)
	require.Len(t, objects, 3)
}

func TestDistributedBackend_ListObjectsBadBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	_, err := b.ListObjects(context.Background(), "nope", "", 100)
	require.ErrorIs(t, err, storage.ErrBucketNotFound)
}

func TestDistributedBackend_WalkObjects(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	for _, kv := range []struct{ key, val string }{
		{"docs/a.txt", "a"},
		{"docs/b.txt", "b"},
		{"images/c.png", "c"},
	} {
		_, err := b.PutObject(context.Background(), "bucket", kv.key, strings.NewReader(kv.val), "text/plain")
		require.NoError(t, err)
	}

	var all []*storage.Object
	err := b.WalkObjects(context.Background(), "bucket", "", func(obj *storage.Object) error {
		all = append(all, obj)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, all, 3)

	var docs []*storage.Object
	err = b.WalkObjects(context.Background(), "bucket", "docs/", func(obj *storage.Object) error {
		docs = append(docs, obj)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, docs, 2)
}

func TestDistributedBackend_WalkObjectsBadBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	err := b.WalkObjects(context.Background(), "nope", "", func(*storage.Object) error { return nil })
	require.ErrorIs(t, err, storage.ErrBucketNotFound)
}

func TestDistributedBackend_WalkObjectsDeletedSkipped(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	_, err := b.PutObject(context.Background(), "bucket", "keep.txt", strings.NewReader("keep"), "text/plain")
	require.NoError(t, err)
	_, err = b.PutObject(context.Background(), "bucket", "gone.txt", strings.NewReader("gone"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, b.DeleteObject(context.Background(), "bucket", "gone.txt"))

	var keys []string
	err = b.WalkObjects(context.Background(), "bucket", "", func(obj *storage.Object) error {
		keys = append(keys, obj.Key)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"keep.txt"}, keys)
}

func TestDistributedBackend_WalkObjectsVersionedLatestOnly(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	_, err := b.PutObject(context.Background(), "bucket", "f.txt", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)
	_, err = b.PutObject(context.Background(), "bucket", "f.txt", strings.NewReader("v2-longer"), "text/plain")
	require.NoError(t, err)

	var objs []*storage.Object
	err = b.WalkObjects(context.Background(), "bucket", "", func(obj *storage.Object) error {
		objs = append(objs, obj)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, objs, 1, "only latest version should be emitted")
	require.Equal(t, int64(9), objs[0].Size, "should report latest version size")
}

func TestDistributedBackend_WalkObjectsEarlyStop(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	for i := range 5 {
		_, err := b.PutObject(context.Background(), "bucket", fmt.Sprintf("f%d.txt", i), strings.NewReader("x"), "text/plain")
		require.NoError(t, err)
	}

	sentinel := fmt.Errorf("stop")
	count := 0
	err := b.WalkObjects(context.Background(), "bucket", "", func(*storage.Object) error {
		count++
		if count == 2 {
			return sentinel
		}
		return nil
	})
	require.ErrorIs(t, err, sentinel)
	require.Equal(t, 2, count)
}

func TestDistributedBackend_Overwrite(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	_, err := b.PutObject(context.Background(), "bucket", "file.txt", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)

	_, err = b.PutObject(context.Background(), "bucket", "file.txt", strings.NewReader("version2"), "text/plain")
	require.NoError(t, err)

	rc, obj, err := b.GetObject(context.Background(), "bucket", "file.txt")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	require.Equal(t, "version2", string(data))
	require.Equal(t, int64(8), obj.Size)
}

func TestDistributedBackend_NestedKey(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	_, err := b.PutObject(context.Background(), "bucket", "a/b/c/deep.txt", strings.NewReader("deep"), "text/plain")
	require.NoError(t, err)

	rc, _, err := b.GetObject(context.Background(), "bucket", "a/b/c/deep.txt")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	require.Equal(t, "deep", string(data))
}

func TestDistributedBackend_MultipartComplete(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	part1 := bytes.Repeat([]byte("A"), 1024)
	part2 := bytes.Repeat([]byte("B"), 512)

	upload, err := b.CreateMultipartUpload(context.Background(), "bucket", "mp.bin", "application/octet-stream")
	require.NoError(t, err)
	require.NotEmpty(t, upload.UploadID)

	p1, err := b.UploadPart(context.Background(), "bucket", "mp.bin", upload.UploadID, 1, bytes.NewReader(part1))
	require.NoError(t, err)
	require.Equal(t, 1, p1.PartNumber)
	require.Equal(t, int64(1024), p1.Size)

	p2, err := b.UploadPart(context.Background(), "bucket", "mp.bin", upload.UploadID, 2, bytes.NewReader(part2))
	require.NoError(t, err)

	obj, err := b.CompleteMultipartUpload(context.Background(), "bucket", "mp.bin", upload.UploadID, []storage.Part{
		{PartNumber: p1.PartNumber, ETag: p1.ETag, Size: p1.Size},
		{PartNumber: p2.PartNumber, ETag: p2.ETag, Size: p2.Size},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1536), obj.Size)

	rc, _, err := b.GetObject(context.Background(), "bucket", "mp.bin")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	require.Equal(t, append(part1, part2...), data)
}

func TestDistributedBackend_MultipartAbort(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	upload, err := b.CreateMultipartUpload(context.Background(), "bucket", "abort.bin", "application/octet-stream")
	require.NoError(t, err)

	_, err = b.UploadPart(context.Background(), "bucket", "abort.bin", upload.UploadID, 1, strings.NewReader("data"))
	require.NoError(t, err)

	require.NoError(t, b.AbortMultipartUpload(context.Background(), "bucket", "abort.bin", upload.UploadID))

	_, err = b.HeadObject(context.Background(), "bucket", "abort.bin")
	require.ErrorIs(t, err, storage.ErrObjectNotFound)
}

func TestDistributedBackend_MultipartBadUploadID(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	_, err := b.UploadPart(context.Background(), "bucket", "file.bin", "bad-id", 1, strings.NewReader("data"))
	require.ErrorIs(t, err, storage.ErrUploadNotFound)

	err = b.AbortMultipartUpload(context.Background(), "bucket", "file.bin", "bad-id")
	require.ErrorIs(t, err, storage.ErrUploadNotFound)

	_, err = b.CompleteMultipartUpload(context.Background(), "bucket", "file.bin", "bad-id", nil)
	require.ErrorIs(t, err, storage.ErrUploadNotFound)
}

func TestDistributedBackend_MultipartBadBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	_, err := b.CreateMultipartUpload(context.Background(), "nope", "file.bin", "application/octet-stream")
	require.ErrorIs(t, err, storage.ErrBucketNotFound)
}

func TestDistributedBackend_Close(t *testing.T) {
	dir := t.TempDir()

	metaDir := dir + "/meta"
	dbOpts := badger.DefaultOptions(metaDir).WithLogger(nil)
	db, err := badger.Open(dbOpts)
	require.NoError(t, err)

	cfg := raft.DefaultConfig("test-node", nil)
	node, closeRaft, err := newRaftNode(cfg, dir)
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
	defer node.Close()
	defer func() {
		if closeRaft != nil {
			_ = closeRaft()
		}
	}()

	backend, err := NewDistributedBackend(dir, db, node, nil, false)
	require.NoError(t, err)

	err = backend.Close()
	require.NoError(t, err)
}

func TestSelectPeerByLoad_ReturnsLightestWhenOverloaded(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", RequestsPerSec: 300.0}) // overloaded
	store.Set(NodeStats{NodeID: "node-b", RequestsPerSec: 50.0})
	store.Set(NodeStats{NodeID: "node-c", RequestsPerSec: 80.0})

	peer, ok := selectPeerByLoad(store, "node-a", 1.3)
	require.True(t, ok)
	require.Equal(t, "node-b", peer) // lowest load
}

func TestSelectPeerByLoad_NoRedirectWhenBalanced(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", RequestsPerSec: 100.0})
	store.Set(NodeStats{NodeID: "node-b", RequestsPerSec: 90.0})
	store.Set(NodeStats{NodeID: "node-c", RequestsPerSec: 110.0})

	// median ~100, node-a = 100, threshold 1.3 → 100 <= 100*1.3 → no redirect
	_, ok := selectPeerByLoad(store, "node-a", 1.3)
	require.False(t, ok)
}

func TestSelectPeerByLoad_SingleNode(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", RequestsPerSec: 1000.0})

	_, ok := selectPeerByLoad(store, "node-a", 1.3)
	require.False(t, ok, "single node: no peers to redirect to")
}

type mockBucketAssigner struct {
	fn func(ctx context.Context, bucket, groupID string) error
}

func (m *mockBucketAssigner) ProposeBucketAssignment(ctx context.Context, bucket, groupID string) error {
	return m.fn(ctx, bucket, groupID)
}

func TestDistributedBackend_SetBucketAssigner_NilNoPanic(t *testing.T) {
	b := newTestDistributedBackend(t)
	b.SetBucketAssigner(nil)
	require.NoError(t, b.CreateBucket(context.Background(), "photos"))
}

func TestDistributedBackend_CreateBucket_AssignerWithoutRouter_Errors(t *testing.T) {
	b := newTestDistributedBackend(t)
	b.SetBucketAssigner(&mockBucketAssigner{fn: func(ctx context.Context, bucket, groupID string) error {
		return nil
	}})
	// router not set → must return an error, not panic
	err := b.CreateBucket(context.Background(), "photos")
	require.Error(t, err)
	require.Contains(t, err.Error(), "router not configured")
}

func TestDistributedBackend_CreateBucket_CallsAssigner(t *testing.T) {
	b := newTestDistributedBackend(t)

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroup("group-0", []string{"node-0"}))
	r := NewRouter(mgr)
	r.SetDefault("group-0")
	b.SetRouter(r)

	var calledBucket, calledGroup string
	b.SetBucketAssigner(&mockBucketAssigner{fn: func(ctx context.Context, bucket, groupID string) error {
		calledBucket = bucket
		calledGroup = groupID
		return nil
	}})

	require.NoError(t, b.CreateBucket(context.Background(), "photos"))
	require.Equal(t, "photos", calledBucket)
	require.Equal(t, "group-0", calledGroup)
}

func TestDistributedBackend_CreateBucket_AssignsBeforeStrictRoute(t *testing.T) {
	b := newTestDistributedBackend(t)
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroup("group-0", []string{"node-0"}))
	r := NewRouter(mgr)
	r.SetDefault("group-0")
	r.SetRequireExplicitAssignments(true)

	b.SetRouter(r)
	b.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-0": {ID: "group-0", PeerIDs: []string{"node-0"}},
	}})
	var assignedBucket, assignedGroup string
	b.SetBucketAssigner(&mockBucketAssigner{fn: func(ctx context.Context, bucket, groupID string) error {
		assignedBucket = bucket
		assignedGroup = groupID
		return nil
	}})

	require.NoError(t, b.CreateBucket(context.Background(), "photos"))
	require.Equal(t, "photos", assignedBucket)
	require.Equal(t, "group-0", assignedGroup)
	r.AssignBucket(assignedBucket, assignedGroup)

	g, err := r.RouteKey("photos", "image.jpg")
	require.NoError(t, err)
	require.Equal(t, "group-0", g.ID())
}

func TestDistributedBackend_CreateBucket_RouterError_Propagates(t *testing.T) {
	b := newTestDistributedBackend(t)

	// Router with no default and no assignment → RouteKey returns ErrNoGroup
	mgr := NewDataGroupManager()
	r := NewRouter(mgr)
	b.SetRouter(r)
	b.SetBucketAssigner(&mockBucketAssigner{fn: func(ctx context.Context, bucket, groupID string) error {
		return nil
	}})

	err := b.CreateBucket(context.Background(), "photos")
	require.Error(t, err)
}
