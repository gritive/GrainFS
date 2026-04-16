package packblob

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func newTestPackedBackend(t *testing.T) *PackedBackend {
	t.Helper()
	dir := t.TempDir()
	inner, err := storage.NewLocalBackend(dir + "/local")
	require.NoError(t, err)

	pb, err := NewPackedBackend(inner, dir+"/blobs", 64*1024) // 64KB threshold
	require.NoError(t, err)
	t.Cleanup(func() { pb.Close() })
	return pb
}

func TestPackedBackend_SmallObjectGoesToBlob(t *testing.T) {
	pb := newTestPackedBackend(t)
	require.NoError(t, pb.CreateBucket("test"))

	// Small object (< 64KB threshold) → packed into blob
	obj, err := pb.PutObject("test", "small.txt", strings.NewReader("tiny data"), "text/plain")
	require.NoError(t, err)
	assert.Equal(t, int64(9), obj.Size)

	rc, gotObj, err := pb.GetObject("test", "small.txt")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, "tiny data", string(data))
	assert.Equal(t, obj.ETag, gotObj.ETag)
}

func TestPackedBackend_LargeObjectPassesThrough(t *testing.T) {
	pb := newTestPackedBackend(t)
	require.NoError(t, pb.CreateBucket("test"))

	// Large object (>= 64KB) → flat file via inner backend
	largeData := strings.Repeat("X", 65*1024)
	obj, err := pb.PutObject("test", "large.bin", strings.NewReader(largeData), "application/octet-stream")
	require.NoError(t, err)
	assert.Equal(t, int64(65*1024), obj.Size)

	rc, _, err := pb.GetObject("test", "large.bin")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, len(largeData), len(data))
}

func TestPackedBackend_DeleteSmallObject(t *testing.T) {
	pb := newTestPackedBackend(t)
	require.NoError(t, pb.CreateBucket("test"))

	_, err := pb.PutObject("test", "del.txt", strings.NewReader("delete me"), "text/plain")
	require.NoError(t, err)

	require.NoError(t, pb.DeleteObject("test", "del.txt"))

	_, err = pb.HeadObject("test", "del.txt")
	assert.ErrorIs(t, err, storage.ErrObjectNotFound)
}

func TestPackedBackend_HeadObject(t *testing.T) {
	pb := newTestPackedBackend(t)
	require.NoError(t, pb.CreateBucket("test"))

	_, err := pb.PutObject("test", "meta.txt", strings.NewReader("metadata"), "text/plain")
	require.NoError(t, err)

	obj, err := pb.HeadObject("test", "meta.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(8), obj.Size)
	assert.Equal(t, "meta.txt", obj.Key)
}

func TestPackedBackend_ListObjects(t *testing.T) {
	pb := newTestPackedBackend(t)
	require.NoError(t, pb.CreateBucket("test"))

	for _, kv := range []struct{ key, val string }{
		{"a.txt", "aaa"},
		{"b.txt", "bbb"},
		{"c.txt", "ccc"},
	} {
		_, err := pb.PutObject("test", kv.key, strings.NewReader(kv.val), "text/plain")
		require.NoError(t, err)
	}

	objects, err := pb.ListObjects("test", "", 100)
	require.NoError(t, err)
	assert.Len(t, objects, 3)
}

func TestPackedBackend_BucketOperations(t *testing.T) {
	pb := newTestPackedBackend(t)

	require.NoError(t, pb.CreateBucket("mybucket"))
	require.NoError(t, pb.HeadBucket("mybucket"))

	buckets, err := pb.ListBuckets()
	require.NoError(t, err)
	assert.Contains(t, buckets, "mybucket")

	require.NoError(t, pb.DeleteBucket("mybucket"))
	assert.ErrorIs(t, pb.HeadBucket("mybucket"), storage.ErrBucketNotFound)
}
