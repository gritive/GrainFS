package packblob

import (
	"context"
	"fmt"
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
	require.NoError(t, pb.CreateBucket(context.Background(), "test"))

	// Small object (< 64KB threshold) → packed into blob
	obj, err := pb.PutObject(context.Background(), "test", "small.txt", strings.NewReader("tiny data"), "text/plain")
	require.NoError(t, err)
	assert.Equal(t, int64(9), obj.Size)

	rc, gotObj, err := pb.GetObject(context.Background(), "test", "small.txt")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, "tiny data", string(data))
	assert.Equal(t, obj.ETag, gotObj.ETag)
}

func TestPackedBackend_LargeObjectPassesThrough(t *testing.T) {
	pb := newTestPackedBackend(t)
	require.NoError(t, pb.CreateBucket(context.Background(), "test"))

	// Large object (>= 64KB) → flat file via inner backend
	largeData := strings.Repeat("X", 65*1024)
	obj, err := pb.PutObject(context.Background(), "test", "large.bin", strings.NewReader(largeData), "application/octet-stream")
	require.NoError(t, err)
	assert.Equal(t, int64(65*1024), obj.Size)

	rc, _, err := pb.GetObject(context.Background(), "test", "large.bin")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, len(largeData), len(data))
}

func TestPackedBackend_DeleteSmallObject(t *testing.T) {
	pb := newTestPackedBackend(t)
	require.NoError(t, pb.CreateBucket(context.Background(), "test"))

	_, err := pb.PutObject(context.Background(), "test", "del.txt", strings.NewReader("delete me"), "text/plain")
	require.NoError(t, err)

	require.NoError(t, pb.DeleteObject(context.Background(), "test", "del.txt"))

	_, err = pb.HeadObject(context.Background(), "test", "del.txt")
	assert.ErrorIs(t, err, storage.ErrObjectNotFound)
}

func TestPackedBackend_HeadObject(t *testing.T) {
	pb := newTestPackedBackend(t)
	require.NoError(t, pb.CreateBucket(context.Background(), "test"))

	_, err := pb.PutObject(context.Background(), "test", "meta.txt", strings.NewReader("metadata"), "text/plain")
	require.NoError(t, err)

	obj, err := pb.HeadObject(context.Background(), "test", "meta.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(8), obj.Size)
	assert.Equal(t, "meta.txt", obj.Key)
}

func TestPackedBackend_ListObjects(t *testing.T) {
	pb := newTestPackedBackend(t)
	require.NoError(t, pb.CreateBucket(context.Background(), "test"))

	for _, kv := range []struct{ key, val string }{
		{"a.txt", "aaa"},
		{"b.txt", "bbb"},
		{"c.txt", "ccc"},
	} {
		_, err := pb.PutObject(context.Background(), "test", kv.key, strings.NewReader(kv.val), "text/plain")
		require.NoError(t, err)
	}

	objects, err := pb.ListObjects(context.Background(), "test", "", 100)
	require.NoError(t, err)
	assert.Len(t, objects, 3)
}

func TestPackedBackend_BucketOperations(t *testing.T) {
	pb := newTestPackedBackend(t)

	require.NoError(t, pb.CreateBucket(context.Background(), "mybucket"))
	require.NoError(t, pb.HeadBucket(context.Background(), "mybucket"))

	buckets, err := pb.ListBuckets(context.Background())
	require.NoError(t, err)
	assert.Contains(t, buckets, "mybucket")

	require.NoError(t, pb.DeleteBucket(context.Background(), "mybucket"))
	assert.ErrorIs(t, pb.HeadBucket(context.Background(), "mybucket"), storage.ErrBucketNotFound)
}

func TestPackedBackend_WalkObjectsPackedObjects(t *testing.T) {
	pb := newTestPackedBackend(t)
	require.NoError(t, pb.CreateBucket(context.Background(), "test"))

	for _, kv := range []struct{ key, val string }{
		{"a.txt", "aaa"},
		{"b.txt", "bbbbb"},
		{"c.txt", "cc"},
	} {
		_, err := pb.PutObject(context.Background(), "test", kv.key, strings.NewReader(kv.val), "text/plain")
		require.NoError(t, err)
	}

	var keys []string
	err := pb.WalkObjects(context.Background(), "test", "", func(obj *storage.Object) error {
		keys = append(keys, obj.Key)
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, keys, 3)
	assert.ElementsMatch(t, []string{"a.txt", "b.txt", "c.txt"}, keys)
}

func TestPackedBackend_WalkObjectsSizeFixup(t *testing.T) {
	pb := newTestPackedBackend(t)
	require.NoError(t, pb.CreateBucket(context.Background(), "test"))

	content := "hello world"
	_, err := pb.PutObject(context.Background(), "test", "file.txt", strings.NewReader(content), "text/plain")
	require.NoError(t, err)

	var objs []*storage.Object
	err = pb.WalkObjects(context.Background(), "test", "", func(obj *storage.Object) error {
		objs = append(objs, obj)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, objs, 1)
	assert.Equal(t, int64(len(content)), objs[0].Size, "packed object size should reflect original content")
}

func TestPackedBackend_WalkObjectsPrefix(t *testing.T) {
	pb := newTestPackedBackend(t)
	require.NoError(t, pb.CreateBucket(context.Background(), "test"))

	for _, kv := range []struct{ key, val string }{
		{"docs/a.txt", "a"},
		{"docs/b.txt", "b"},
		{"images/c.png", "c"},
	} {
		_, err := pb.PutObject(context.Background(), "test", kv.key, strings.NewReader(kv.val), "text/plain")
		require.NoError(t, err)
	}

	var keys []string
	err := pb.WalkObjects(context.Background(), "test", "docs/", func(obj *storage.Object) error {
		keys = append(keys, obj.Key)
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, keys, 2)
	for _, k := range keys {
		assert.True(t, strings.HasPrefix(k, "docs/"), "unexpected key: %s", k)
	}
}

func TestPackedBackend_WalkObjectsEarlyStop(t *testing.T) {
	pb := newTestPackedBackend(t)
	require.NoError(t, pb.CreateBucket(context.Background(), "test"))

	for i := range 5 {
		_, err := pb.PutObject(context.Background(), "test", strings.Repeat(string(rune('a'+i)), 1)+"_file.txt",
			strings.NewReader("x"), "text/plain")
		require.NoError(t, err)
	}

	sentinel := fmt.Errorf("stop")
	count := 0
	err := pb.WalkObjects(context.Background(), "test", "", func(*storage.Object) error {
		count++
		if count == 2 {
			return sentinel
		}
		return nil
	})
	assert.ErrorIs(t, err, sentinel)
	assert.Equal(t, 2, count)
}
