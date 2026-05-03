package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
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

func (s *blockingSnapshotter) Restore([]byte) error {
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

	raftDir := dir + "/raft"
	logStore, err := raft.NewBadgerLogStore(raftDir)
	require.NoError(t, err)

	cfg := raft.DefaultConfig("test-node", nil)
	node := raft.NewNode(cfg, logStore)
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
	require.Equal(t, raft.Leader, node.State(), "no-peers node must become leader")

	backend, err := NewDistributedBackend(dir, db, node)
	require.NoError(t, err)

	stopApply := make(chan struct{})
	go backend.RunApplyLoop(stopApply)

	t.Cleanup(func() {
		close(stopApply)
		node.Stop()
		db.Close()
		logStore.Close()
	})

	return backend
}

func TestDistributedBackend_CreateAndHeadBucket(t *testing.T) {
	b := newTestDistributedBackend(t)

	require.NoError(t, b.CreateBucket(context.Background(), "test"))
	require.NoError(t, b.HeadBucket(context.Background(), "test"))
	assert.ErrorIs(t, b.HeadBucket(context.Background(), "nope"), storage.ErrBucketNotFound)
}

func TestDistributedBackend_CreateBucketConflict(t *testing.T) {
	b := newTestDistributedBackend(t)

	require.NoError(t, b.CreateBucket(context.Background(), "dup"))
	assert.ErrorIs(t, b.CreateBucket(context.Background(), "dup"), storage.ErrBucketAlreadyExists)
}

func TestDistributedBackend_ListBuckets(t *testing.T) {
	b := newTestDistributedBackend(t)

	require.NoError(t, b.CreateBucket(context.Background(), "alpha"))
	require.NoError(t, b.CreateBucket(context.Background(), "beta"))

	buckets, err := b.ListBuckets(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta"}, buckets)
}

func TestDistributedBackend_DeleteBucket(t *testing.T) {
	b := newTestDistributedBackend(t)

	require.NoError(t, b.CreateBucket(context.Background(), "del"))
	require.NoError(t, b.DeleteBucket(context.Background(), "del"))
	assert.ErrorIs(t, b.HeadBucket(context.Background(), "del"), storage.ErrBucketNotFound)
}

func TestDistributedBackend_DeleteBucketNotFound(t *testing.T) {
	b := newTestDistributedBackend(t)
	assert.ErrorIs(t, b.DeleteBucket(context.Background(), "nope"), storage.ErrBucketNotFound)
}

func TestDistributedBackend_DeleteBucketNotEmpty(t *testing.T) {
	b := newTestDistributedBackend(t)

	require.NoError(t, b.CreateBucket(context.Background(), "notempty"))
	_, err := b.PutObject(context.Background(), "notempty", "file.txt", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)

	assert.ErrorIs(t, b.DeleteBucket(context.Background(), "notempty"), storage.ErrBucketNotEmpty)
}

func TestDistributedBackend_PutAndGetObject(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	obj, err := b.PutObject(context.Background(), "bucket", "hello.txt", strings.NewReader("hello world"), "text/plain")
	require.NoError(t, err)
	assert.Equal(t, int64(11), obj.Size)
	assert.Equal(t, "text/plain", obj.ContentType)
	assert.NotEmpty(t, obj.ETag)

	rc, gotObj, err := b.GetObject(context.Background(), "bucket", "hello.txt")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, "hello world", string(data))
	assert.Equal(t, obj.ETag, gotObj.ETag)
	assert.Equal(t, obj.Size, gotObj.Size)
}

func TestDistributedBackend_PutObjectToBadBucket(t *testing.T) {
	b := newTestDistributedBackend(t)

	_, err := b.PutObject(context.Background(), "nope", "file.txt", strings.NewReader("data"), "text/plain")
	assert.ErrorIs(t, err, storage.ErrBucketNotFound)
}

func TestDistributedBackend_HeadObject(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	_, err := b.PutObject(context.Background(), "bucket", "meta.txt", strings.NewReader("metadata"), "text/plain")
	require.NoError(t, err)

	obj, err := b.HeadObject(context.Background(), "bucket", "meta.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(8), obj.Size)
	assert.Equal(t, "meta.txt", obj.Key)
}

func TestDistributedBackend_HeadObjectNotFound(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	_, err := b.HeadObject(context.Background(), "bucket", "nope.txt")
	assert.ErrorIs(t, err, storage.ErrObjectNotFound)
}

func TestDistributedBackend_DeleteObject(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	_, err := b.PutObject(context.Background(), "bucket", "del.txt", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)

	require.NoError(t, b.DeleteObject(context.Background(), "bucket", "del.txt"))

	_, err = b.HeadObject(context.Background(), "bucket", "del.txt")
	assert.ErrorIs(t, err, storage.ErrObjectNotFound)
}

func TestDistributedBackend_DeleteObjectBadBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	assert.ErrorIs(t, b.DeleteObject(context.Background(), "nope", "file.txt"), storage.ErrBucketNotFound)
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
	assert.Len(t, objects, 3)

	objects, err = b.ListObjects(context.Background(), "bucket", "docs/", 100)
	require.NoError(t, err)
	assert.Len(t, objects, 2)
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
	assert.Len(t, objects, 3)
}

func TestDistributedBackend_ListObjectsBadBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	_, err := b.ListObjects(context.Background(), "nope", "", 100)
	assert.ErrorIs(t, err, storage.ErrBucketNotFound)
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
	assert.Len(t, all, 3)

	var docs []*storage.Object
	err = b.WalkObjects(context.Background(), "bucket", "docs/", func(obj *storage.Object) error {
		docs = append(docs, obj)
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, docs, 2)
}

func TestDistributedBackend_WalkObjectsBadBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	err := b.WalkObjects(context.Background(), "nope", "", func(*storage.Object) error { return nil })
	assert.ErrorIs(t, err, storage.ErrBucketNotFound)
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
	assert.Equal(t, []string{"keep.txt"}, keys)
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
	assert.Equal(t, int64(9), objs[0].Size, "should report latest version size")
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
	assert.ErrorIs(t, err, sentinel)
	assert.Equal(t, 2, count)
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
	assert.Equal(t, "version2", string(data))
	assert.Equal(t, int64(8), obj.Size)
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
	assert.Equal(t, "deep", string(data))
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
	assert.Equal(t, 1, p1.PartNumber)
	assert.Equal(t, int64(1024), p1.Size)

	p2, err := b.UploadPart(context.Background(), "bucket", "mp.bin", upload.UploadID, 2, bytes.NewReader(part2))
	require.NoError(t, err)

	obj, err := b.CompleteMultipartUpload(context.Background(), "bucket", "mp.bin", upload.UploadID, []storage.Part{
		{PartNumber: p1.PartNumber, ETag: p1.ETag, Size: p1.Size},
		{PartNumber: p2.PartNumber, ETag: p2.ETag, Size: p2.Size},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1536), obj.Size)

	rc, _, err := b.GetObject(context.Background(), "bucket", "mp.bin")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, append(part1, part2...), data)
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
	assert.ErrorIs(t, err, storage.ErrObjectNotFound)
}

func TestDistributedBackend_MultipartBadUploadID(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))

	_, err := b.UploadPart(context.Background(), "bucket", "file.bin", "bad-id", 1, strings.NewReader("data"))
	assert.ErrorIs(t, err, storage.ErrUploadNotFound)

	err = b.AbortMultipartUpload(context.Background(), "bucket", "file.bin", "bad-id")
	assert.ErrorIs(t, err, storage.ErrUploadNotFound)

	_, err = b.CompleteMultipartUpload(context.Background(), "bucket", "file.bin", "bad-id", nil)
	assert.ErrorIs(t, err, storage.ErrUploadNotFound)
}

func TestDistributedBackend_MultipartBadBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	_, err := b.CreateMultipartUpload(context.Background(), "nope", "file.bin", "application/octet-stream")
	assert.ErrorIs(t, err, storage.ErrBucketNotFound)
}

func TestDistributedBackend_SnapshotTriggersAfterThreshold(t *testing.T) {
	dir := t.TempDir()

	metaDir := dir + "/meta"
	dbOpts := badger.DefaultOptions(metaDir).WithLogger(nil)
	db, err := badger.Open(dbOpts)
	require.NoError(t, err)

	raftDir := dir + "/raft"
	logStore, err := raft.NewBadgerLogStore(raftDir)
	require.NoError(t, err)

	cfg := raft.DefaultConfig("test-node", nil)
	node := raft.NewNode(cfg, logStore)
	node.SetTransport(
		func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
			return nil, fmt.Errorf("no peers")
		},
		func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
			return nil, fmt.Errorf("no peers")
		},
	)
	node.Start()

	require.Eventually(t, func() bool {
		return node.State() == raft.Leader
	}, 3*time.Second, 10*time.Millisecond)

	backend, err := NewDistributedBackend(dir, db, node)
	require.NoError(t, err)

	// Set snapshot threshold to 5 entries
	fsm := NewFSM(db)
	snapMgr := raft.NewSnapshotManager(logStore, fsm, raft.SnapshotConfig{Threshold: 5})
	backend.SetSnapshotManager(snapMgr, node)

	stopApply := make(chan struct{})
	go backend.RunApplyLoop(stopApply)

	t.Cleanup(func() {
		close(stopApply)
		node.Stop()
		db.Close()
		logStore.Close()
	})

	// Create 6 buckets (6 Raft entries, exceeding threshold of 5)
	for i := range 6 {
		require.NoError(t, backend.CreateBucket(context.Background(), fmt.Sprintf("snap-bucket-%d", i)))
	}

	// Wait for entries to be applied
	time.Sleep(500 * time.Millisecond)

	// Verify snapshot was saved
	snap, err := logStore.LoadSnapshot()
	require.NoError(t, err)
	assert.Greater(t, snap.Index, uint64(0), "snapshot should have been saved")
	assert.NotNil(t, snap.Data, "snapshot data should exist")
}

func TestDistributedBackend_TriggerRaftSnapshotLeader(t *testing.T) {
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir + "/meta").WithLogger(nil))
	require.NoError(t, err)
	logStore, err := raft.NewBadgerLogStore(dir + "/raft")
	require.NoError(t, err)
	node := raft.NewNode(raft.DefaultConfig("leader", nil), logStore)
	node.SetTransport(
		func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
			return nil, fmt.Errorf("no peers")
		},
		func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
			return nil, fmt.Errorf("no peers")
		},
	)
	node.Start()
	require.Eventually(t, func() bool { return node.State() == raft.Leader }, 3*time.Second, 10*time.Millisecond)
	backend, err := NewDistributedBackend(dir, db, node)
	require.NoError(t, err)
	backend.SetSnapshotManager(raft.NewSnapshotManager(logStore, backend.fsm, raft.SnapshotConfig{Threshold: 100}), node)

	stopApply := make(chan struct{})
	go backend.RunApplyLoop(stopApply)
	t.Cleanup(func() {
		close(stopApply)
		node.Stop()
		db.Close()
		logStore.Close()
	})

	require.NoError(t, backend.CreateBucket(context.Background(), "manual-snap"))
	require.Eventually(t, func() bool {
		return backend.lastApplied.Load() > 0
	}, 3*time.Second, 10*time.Millisecond)

	result, err := backend.TriggerRaftSnapshot(context.Background())
	require.NoError(t, err)
	assert.Equal(t, backend.lastApplied.Load(), result.Index)

	status, err := backend.RaftSnapshotStatus()
	require.NoError(t, err)
	assert.True(t, status.Available)
	assert.Equal(t, result.Index, status.Index)
}

func TestDistributedBackend_TriggerRaftSnapshotSerializesWithApplyLoop(t *testing.T) {
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir + "/meta").WithLogger(nil))
	require.NoError(t, err)
	logStore, err := raft.NewBadgerLogStore(dir + "/raft")
	require.NoError(t, err)
	node := raft.NewNode(raft.DefaultConfig("leader", nil), logStore)
	node.SetTransport(
		func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
			return nil, fmt.Errorf("no peers")
		},
		func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
			return nil, fmt.Errorf("no peers")
		},
	)
	node.Start()
	require.Eventually(t, func() bool { return node.State() == raft.Leader }, 3*time.Second, 10*time.Millisecond)
	backend, err := NewDistributedBackend(dir, db, node)
	require.NoError(t, err)

	stopApply := make(chan struct{})
	go backend.RunApplyLoop(stopApply)
	t.Cleanup(func() {
		close(stopApply)
		node.Stop()
		db.Close()
		logStore.Close()
	})

	require.NoError(t, backend.CreateBucket(context.Background(), "before-snapshot"))
	initialApplied := backend.lastApplied.Load()
	require.NotZero(t, initialApplied)

	blockingSnap := newBlockingSnapshotter()
	backend.SetSnapshotManager(raft.NewSnapshotManager(logStore, blockingSnap, raft.SnapshotConfig{Threshold: 100}), node)

	triggerDone := make(chan error, 1)
	go func() {
		_, err := backend.TriggerRaftSnapshot(context.Background())
		triggerDone <- err
	}()
	require.Eventually(t, func() bool {
		select {
		case <-blockingSnap.entered:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	applyDone := make(chan error, 1)
	go func() {
		applyDone <- backend.CreateBucket(context.Background(), "during-snapshot")
	}()

	assert.Never(t, func() bool {
		return backend.lastApplied.Load() > initialApplied
	}, 150*time.Millisecond, 10*time.Millisecond, "apply loop must not advance while manual snapshot is reading FSM state")
	select {
	case err := <-applyDone:
		require.NoError(t, err)
		t.Fatal("CreateBucket applied before snapshot finished")
	default:
	}

	close(blockingSnap.release)
	require.NoError(t, <-triggerDone)
	require.NoError(t, <-applyDone)
	assert.Greater(t, backend.lastApplied.Load(), initialApplied)
}

func TestDistributedBackend_TriggerRaftSnapshotRejectsFollower(t *testing.T) {
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir + "/meta").WithLogger(nil))
	require.NoError(t, err)
	defer db.Close()
	logStore, err := raft.NewBadgerLogStore(dir + "/raft")
	require.NoError(t, err)
	defer logStore.Close()

	node := raft.NewNode(raft.DefaultConfig("follower", []string{"leader"}), logStore)
	backend, err := NewDistributedBackend(dir, db, node)
	require.NoError(t, err)
	backend.SetSnapshotManager(raft.NewSnapshotManager(logStore, backend.fsm, raft.SnapshotConfig{Threshold: 100}), node)

	_, err = backend.TriggerRaftSnapshot(context.Background())
	assert.ErrorIs(t, err, raft.ErrNotLeader)
}

func TestDistributedBackend_Close(t *testing.T) {
	dir := t.TempDir()

	metaDir := dir + "/meta"
	dbOpts := badger.DefaultOptions(metaDir).WithLogger(nil)
	db, err := badger.Open(dbOpts)
	require.NoError(t, err)

	raftDir := dir + "/raft"
	logStore, err := raft.NewBadgerLogStore(raftDir)
	require.NoError(t, err)

	cfg := raft.DefaultConfig("test-node", nil)
	node := raft.NewNode(cfg, logStore)
	node.SetTransport(
		func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
			return nil, fmt.Errorf("no peers")
		},
		func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
			return nil, fmt.Errorf("no peers")
		},
	)
	node.Start()
	defer node.Stop()
	defer logStore.Close()

	backend, err := NewDistributedBackend(dir, db, node)
	require.NoError(t, err)

	err = backend.Close()
	assert.NoError(t, err)
}

func TestSelectPeerByLoad_ReturnsLightestWhenOverloaded(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", RequestsPerSec: 300.0}) // overloaded
	store.Set(NodeStats{NodeID: "node-b", RequestsPerSec: 50.0})
	store.Set(NodeStats{NodeID: "node-c", RequestsPerSec: 80.0})

	peer, ok := selectPeerByLoad(store, "node-a", 1.3)
	require.True(t, ok)
	assert.Equal(t, "node-b", peer) // lowest load
}

func TestSelectPeerByLoad_NoRedirectWhenBalanced(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", RequestsPerSec: 100.0})
	store.Set(NodeStats{NodeID: "node-b", RequestsPerSec: 90.0})
	store.Set(NodeStats{NodeID: "node-c", RequestsPerSec: 110.0})

	// median ~100, node-a = 100, threshold 1.3 → 100 <= 100*1.3 → no redirect
	_, ok := selectPeerByLoad(store, "node-a", 1.3)
	assert.False(t, ok)
}

func TestSelectPeerByLoad_SingleNode(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", RequestsPerSec: 1000.0})

	_, ok := selectPeerByLoad(store, "node-a", 1.3)
	assert.False(t, ok, "single node: no peers to redirect to")
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
	assert.Contains(t, err.Error(), "router not configured")
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
	assert.Equal(t, "photos", calledBucket)
	assert.Equal(t, "group-0", calledGroup)
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
