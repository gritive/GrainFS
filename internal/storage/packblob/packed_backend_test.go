package packblob

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync/atomic"
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

type countingBackend struct {
	mockBackend
	puts    atomic.Int64
	deletes atomic.Int64
}

func (b *countingBackend) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	b.puts.Add(1)
	return b.mockBackend.PutObject(ctx, bucket, key, r, contentType)
}

func (b *countingBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	b.deletes.Add(1)
	return b.mockBackend.DeleteObject(ctx, bucket, key)
}

func TestPackedBackend_SmallObjectSkipsInnerMarkerWrite(t *testing.T) {
	inner := &countingBackend{}
	pb, err := NewPackedBackend(inner, t.TempDir(), 64*1024)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pb.Close()) })

	_, err = pb.PutObject(context.Background(), "test", "small.txt", strings.NewReader("tiny data"), "text/plain")
	require.NoError(t, err)
	require.Equal(t, int64(0), inner.puts.Load(), "small packed objects should not create per-object marker PUTs")

	require.NoError(t, pb.DeleteObject(context.Background(), "test", "small.txt"))
	require.Equal(t, int64(0), inner.deletes.Load(), "small packed deletes should not call inner object delete without a marker")
}

func TestPackedBackend_DeleteBucketSeesPackedObjects(t *testing.T) {
	pb := newTestPackedBackend(t)
	require.NoError(t, pb.CreateBucket(context.Background(), "test"))
	_, err := pb.PutObject(context.Background(), "test", "small.txt", strings.NewReader("tiny data"), "text/plain")
	require.NoError(t, err)

	require.ErrorIs(t, pb.DeleteBucket(context.Background(), "test"), storage.ErrBucketNotEmpty)

	require.NoError(t, pb.ForceDeleteBucket(context.Background(), "test"))
	require.ErrorIs(t, pb.HeadBucket(context.Background(), "test"), storage.ErrBucketNotFound)
	_, packed := pb.index.Load(packedKey{bucket: "test", key: "small.txt"})
	require.False(t, packed, "force delete should remove packed index entries for the bucket")
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

func TestPackedBackend_SmallObjectWithUserMetadataGoesToBlob(t *testing.T) {
	pb := newTestPackedBackend(t)
	require.NoError(t, pb.CreateBucket(context.Background(), "test"))

	obj, err := pb.PutObjectWithUserMetadata(
		context.Background(),
		"test",
		"small-meta.txt",
		strings.NewReader("tiny data"),
		"text/plain",
		map[string]string{"origin": "s3"},
	)
	require.NoError(t, err)
	require.Equal(t, int64(9), obj.Size)
	require.Equal(t, map[string]string{"origin": "s3"}, obj.UserMetadata)

	_, packed := pb.index.Load(packedKey{bucket: "test", key: "small-meta.txt"})
	require.True(t, packed, "metadata-aware small PUT should still use blob storage")

	head, err := pb.HeadObject(context.Background(), "test", "small-meta.txt")
	require.NoError(t, err)
	require.Equal(t, int64(9), head.Size)
	require.Equal(t, map[string]string{"origin": "s3"}, head.UserMetadata)

	rc, gotObj, err := pb.GetObject(context.Background(), "test", "small-meta.txt")
	require.NoError(t, err)
	defer rc.Close()
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "tiny data", string(data))
	require.Equal(t, map[string]string{"origin": "s3"}, gotObj.UserMetadata)
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

type thresholdGuardReader struct {
	data        []byte
	pos         int
	limit       int
	allowBeyond atomic.Bool
}

func (r *thresholdGuardReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	if !r.allowBeyond.Load() {
		remaining := r.limit - r.pos
		if remaining <= 0 {
			return 0, fmt.Errorf("read past threshold before delegation")
		}
		if len(p) > remaining {
			p = p[:remaining]
		}
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

type recordingBackend struct {
	mockBackend
	body  []byte
	guard *thresholdGuardReader
	puts  atomic.Int64
}

func (b *recordingBackend) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	_ = ctx
	_ = bucket
	_ = contentType
	b.puts.Add(1)
	if b.guard != nil {
		b.guard.allowBeyond.Store(true)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	b.body = append(b.body[:0], data...)
	return &storage.Object{
		Key:          key,
		Size:         int64(len(data)),
		ContentType:  contentType,
		ETag:         "recorded-etag",
		LastModified: 1,
	}, nil
}

type streamingBackend struct {
	mockBackend
	size int64
	puts atomic.Int64
}

func (b *streamingBackend) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	_ = ctx
	_ = bucket
	_ = contentType
	b.puts.Add(1)
	n, err := io.Copy(io.Discard, r)
	if err != nil {
		return nil, err
	}
	b.size = n
	return &storage.Object{
		Key:          key,
		Size:         n,
		ContentType:  contentType,
		ETag:         "streamed-etag",
		LastModified: 1,
	}, nil
}

func TestPackedBackend_LargeObjectStreamsAfterThreshold(t *testing.T) {
	const threshold = 8
	body := []byte("123456789")
	reader := &thresholdGuardReader{data: body, limit: threshold}
	inner := &recordingBackend{guard: reader}
	pb, err := NewPackedBackend(inner, t.TempDir(), threshold)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pb.Close()) })

	obj, err := pb.PutObject(context.Background(), "test", "large.bin", reader, "application/octet-stream")
	require.NoError(t, err)
	require.Equal(t, int64(len(body)), obj.Size)
	require.Equal(t, body, inner.body)
	require.Equal(t, int64(1), inner.puts.Load())
}

func TestPackedBackend_ThresholdRouting(t *testing.T) {
	for _, tc := range []struct {
		name       string
		body       []byte
		wantPacked bool
		wantPuts   int64
	}{
		{name: "below threshold packs", body: bytes.Repeat([]byte("a"), 7), wantPacked: true},
		{name: "at threshold passes through", body: bytes.Repeat([]byte("b"), 8), wantPuts: 1},
		{name: "above threshold passes through", body: bytes.Repeat([]byte("c"), 9), wantPuts: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inner := &recordingBackend{}
			pb, err := NewPackedBackend(inner, t.TempDir(), 8)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, pb.Close()) })

			_, err = pb.PutObject(context.Background(), "test", "obj", bytes.NewReader(tc.body), "application/octet-stream")
			require.NoError(t, err)

			_, packed := pb.index.Load(packedKey{bucket: "test", key: "obj"})
			require.Equal(t, tc.wantPacked, packed)
			require.Equal(t, tc.wantPuts, inner.puts.Load())
			if tc.wantPuts > 0 {
				require.Equal(t, tc.body, inner.body)
			}
		})
	}
}

func TestPackedBackend_LargeObjectIntakeAllocationBound(t *testing.T) {
	const threshold = 1024
	body := bytes.Repeat([]byte("x"), 256*1024)
	inner := &streamingBackend{}
	pb, err := NewPackedBackend(inner, t.TempDir(), threshold)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pb.Close()) })

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err = pb.PutObject(context.Background(), "test", "large.bin", bytes.NewReader(body), "application/octet-stream")
	require.NoError(t, err)
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(len(body)/2), "large object intake should not allocate proportional to object size")
	require.Equal(t, int64(len(body)), inner.size)
	require.Equal(t, int64(1), inner.puts.Load())
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
