package cluster

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
)

// newTestDistributedBackend creates a DistributedBackend backed by a solo Raft node.
func newTestDistributedBackend(t *testing.T) *DistributedBackend {
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
	require.Equal(t, raft.Leader, node.State(), "solo node must become leader")

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

	require.NoError(t, b.CreateBucket("test"))
	require.NoError(t, b.HeadBucket("test"))
	assert.ErrorIs(t, b.HeadBucket("nope"), storage.ErrBucketNotFound)
}

func TestDistributedBackend_CreateBucketConflict(t *testing.T) {
	b := newTestDistributedBackend(t)

	require.NoError(t, b.CreateBucket("dup"))
	assert.ErrorIs(t, b.CreateBucket("dup"), storage.ErrBucketAlreadyExists)
}

func TestDistributedBackend_ListBuckets(t *testing.T) {
	b := newTestDistributedBackend(t)

	require.NoError(t, b.CreateBucket("alpha"))
	require.NoError(t, b.CreateBucket("beta"))

	buckets, err := b.ListBuckets()
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta"}, buckets)
}

func TestDistributedBackend_DeleteBucket(t *testing.T) {
	b := newTestDistributedBackend(t)

	require.NoError(t, b.CreateBucket("del"))
	require.NoError(t, b.DeleteBucket("del"))
	assert.ErrorIs(t, b.HeadBucket("del"), storage.ErrBucketNotFound)
}

func TestDistributedBackend_DeleteBucketNotFound(t *testing.T) {
	b := newTestDistributedBackend(t)
	assert.ErrorIs(t, b.DeleteBucket("nope"), storage.ErrBucketNotFound)
}

func TestDistributedBackend_DeleteBucketNotEmpty(t *testing.T) {
	b := newTestDistributedBackend(t)

	require.NoError(t, b.CreateBucket("notempty"))
	_, err := b.PutObject("notempty", "file.txt", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)

	assert.ErrorIs(t, b.DeleteBucket("notempty"), storage.ErrBucketNotEmpty)
}

func TestDistributedBackend_PutAndGetObject(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	obj, err := b.PutObject("bucket", "hello.txt", strings.NewReader("hello world"), "text/plain")
	require.NoError(t, err)
	assert.Equal(t, int64(11), obj.Size)
	assert.Equal(t, "text/plain", obj.ContentType)
	assert.NotEmpty(t, obj.ETag)

	rc, gotObj, err := b.GetObject("bucket", "hello.txt")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, "hello world", string(data))
	assert.Equal(t, obj.ETag, gotObj.ETag)
	assert.Equal(t, obj.Size, gotObj.Size)
}

func TestDistributedBackend_PutObjectToBadBucket(t *testing.T) {
	b := newTestDistributedBackend(t)

	_, err := b.PutObject("nope", "file.txt", strings.NewReader("data"), "text/plain")
	assert.ErrorIs(t, err, storage.ErrBucketNotFound)
}

func TestDistributedBackend_HeadObject(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	_, err := b.PutObject("bucket", "meta.txt", strings.NewReader("metadata"), "text/plain")
	require.NoError(t, err)

	obj, err := b.HeadObject("bucket", "meta.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(8), obj.Size)
	assert.Equal(t, "meta.txt", obj.Key)
}

func TestDistributedBackend_HeadObjectNotFound(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	_, err := b.HeadObject("bucket", "nope.txt")
	assert.ErrorIs(t, err, storage.ErrObjectNotFound)
}

func TestDistributedBackend_DeleteObject(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	_, err := b.PutObject("bucket", "del.txt", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)

	require.NoError(t, b.DeleteObject("bucket", "del.txt"))

	_, err = b.HeadObject("bucket", "del.txt")
	assert.ErrorIs(t, err, storage.ErrObjectNotFound)
}

func TestDistributedBackend_DeleteObjectBadBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	assert.ErrorIs(t, b.DeleteObject("nope", "file.txt"), storage.ErrBucketNotFound)
}

func TestDistributedBackend_ListObjects(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	for _, kv := range []struct{ key, val string }{
		{"docs/a.txt", "a"},
		{"docs/b.txt", "b"},
		{"images/c.png", "c"},
	} {
		_, err := b.PutObject("bucket", kv.key, strings.NewReader(kv.val), "text/plain")
		require.NoError(t, err)
	}

	objects, err := b.ListObjects("bucket", "", 100)
	require.NoError(t, err)
	assert.Len(t, objects, 3)

	objects, err = b.ListObjects("bucket", "docs/", 100)
	require.NoError(t, err)
	assert.Len(t, objects, 2)
}

func TestDistributedBackend_ListObjectsMaxKeys(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	for i := range 5 {
		_, err := b.PutObject("bucket", fmt.Sprintf("file%d.txt", i), strings.NewReader("x"), "text/plain")
		require.NoError(t, err)
	}

	objects, err := b.ListObjects("bucket", "", 3)
	require.NoError(t, err)
	assert.Len(t, objects, 3)
}

func TestDistributedBackend_ListObjectsBadBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	_, err := b.ListObjects("nope", "", 100)
	assert.ErrorIs(t, err, storage.ErrBucketNotFound)
}

func TestDistributedBackend_Overwrite(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	_, err := b.PutObject("bucket", "file.txt", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)

	_, err = b.PutObject("bucket", "file.txt", strings.NewReader("version2"), "text/plain")
	require.NoError(t, err)

	rc, obj, err := b.GetObject("bucket", "file.txt")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, "version2", string(data))
	assert.Equal(t, int64(8), obj.Size)
}

func TestDistributedBackend_NestedKey(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	_, err := b.PutObject("bucket", "a/b/c/deep.txt", strings.NewReader("deep"), "text/plain")
	require.NoError(t, err)

	rc, _, err := b.GetObject("bucket", "a/b/c/deep.txt")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, "deep", string(data))
}

func TestDistributedBackend_MultipartComplete(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	part1 := bytes.Repeat([]byte("A"), 1024)
	part2 := bytes.Repeat([]byte("B"), 512)

	upload, err := b.CreateMultipartUpload("bucket", "mp.bin", "application/octet-stream")
	require.NoError(t, err)
	require.NotEmpty(t, upload.UploadID)

	p1, err := b.UploadPart("bucket", "mp.bin", upload.UploadID, 1, bytes.NewReader(part1))
	require.NoError(t, err)
	assert.Equal(t, 1, p1.PartNumber)
	assert.Equal(t, int64(1024), p1.Size)

	p2, err := b.UploadPart("bucket", "mp.bin", upload.UploadID, 2, bytes.NewReader(part2))
	require.NoError(t, err)

	obj, err := b.CompleteMultipartUpload("bucket", "mp.bin", upload.UploadID, []storage.Part{
		{PartNumber: p1.PartNumber, ETag: p1.ETag, Size: p1.Size},
		{PartNumber: p2.PartNumber, ETag: p2.ETag, Size: p2.Size},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1536), obj.Size)

	rc, _, err := b.GetObject("bucket", "mp.bin")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, append(part1, part2...), data)
}

func TestDistributedBackend_MultipartAbort(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	upload, err := b.CreateMultipartUpload("bucket", "abort.bin", "application/octet-stream")
	require.NoError(t, err)

	_, err = b.UploadPart("bucket", "abort.bin", upload.UploadID, 1, strings.NewReader("data"))
	require.NoError(t, err)

	require.NoError(t, b.AbortMultipartUpload("bucket", "abort.bin", upload.UploadID))

	_, err = b.HeadObject("bucket", "abort.bin")
	assert.ErrorIs(t, err, storage.ErrObjectNotFound)
}

func TestDistributedBackend_MultipartBadUploadID(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	_, err := b.UploadPart("bucket", "file.bin", "bad-id", 1, strings.NewReader("data"))
	assert.ErrorIs(t, err, storage.ErrUploadNotFound)

	err = b.AbortMultipartUpload("bucket", "file.bin", "bad-id")
	assert.ErrorIs(t, err, storage.ErrUploadNotFound)

	_, err = b.CompleteMultipartUpload("bucket", "file.bin", "bad-id", nil)
	assert.ErrorIs(t, err, storage.ErrUploadNotFound)
}

func TestDistributedBackend_MultipartBadBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	_, err := b.CreateMultipartUpload("nope", "file.bin", "application/octet-stream")
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
		require.NoError(t, backend.CreateBucket(fmt.Sprintf("snap-bucket-%d", i)))
	}

	// Wait for entries to be applied
	time.Sleep(500 * time.Millisecond)

	// Verify snapshot was saved
	idx, _, data, err := logStore.LoadSnapshot()
	require.NoError(t, err)
	assert.Greater(t, idx, uint64(0), "snapshot should have been saved")
	assert.NotNil(t, data, "snapshot data should exist")
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
