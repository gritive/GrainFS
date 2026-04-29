package storage

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestBackend(t *testing.T) *LocalBackend {
	t.Helper()
	dir := t.TempDir()
	b, err := NewLocalBackend(dir)
	require.NoError(t, err, "NewLocalBackend")
	t.Cleanup(func() { b.Close() })
	return b
}

func TestCreateBucket(t *testing.T) {
	b := setupTestBackend(t)

	require.NoError(t, b.CreateBucket("test-bucket"), "CreateBucket")

	// duplicate should fail
	require.ErrorIs(t, b.CreateBucket("test-bucket"), ErrBucketAlreadyExists)
}

func TestHeadBucket(t *testing.T) {
	b := setupTestBackend(t)

	require.ErrorIs(t, b.HeadBucket("nonexistent"), ErrBucketNotFound)

	b.CreateBucket("test-bucket")
	require.NoError(t, b.HeadBucket("test-bucket"), "HeadBucket")
}

func TestDeleteBucket(t *testing.T) {
	b := setupTestBackend(t)

	require.ErrorIs(t, b.DeleteBucket("nonexistent"), ErrBucketNotFound)

	b.CreateBucket("test-bucket")
	b.PutObject("test-bucket", "file.txt", bytes.NewReader([]byte("data")), "text/plain")

	require.ErrorIs(t, b.DeleteBucket("test-bucket"), ErrBucketNotEmpty)

	b.DeleteObject("test-bucket", "file.txt")
	require.NoError(t, b.DeleteBucket("test-bucket"), "DeleteBucket")
}

func TestListBuckets(t *testing.T) {
	b := setupTestBackend(t)

	buckets, err := b.ListBuckets()
	require.NoError(t, err, "ListBuckets")
	require.Empty(t, buckets)

	b.CreateBucket("alpha")
	b.CreateBucket("bravo")

	buckets, err = b.ListBuckets()
	require.NoError(t, err, "ListBuckets")
	require.Len(t, buckets, 2)
}

func TestPutAndGetObject(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket("test-bucket")

	data := []byte("hello grainfs")
	obj, err := b.PutObject("test-bucket", "greeting.txt", bytes.NewReader(data), "text/plain")
	require.NoError(t, err, "PutObject")
	assert.Equal(t, int64(len(data)), obj.Size)
	assert.Equal(t, "text/plain", obj.ContentType)
	assert.NotEmpty(t, obj.ETag)

	rc, meta, err := b.GetObject("test-bucket", "greeting.txt")
	require.NoError(t, err, "GetObject")
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	assert.Equal(t, data, got)
	assert.Equal(t, int64(len(data)), meta.Size)
}

func TestGetObjectNotFound(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket("test-bucket")

	_, _, err := b.GetObject("test-bucket", "nope.txt")
	require.ErrorIs(t, err, ErrObjectNotFound)
}

func TestGetObjectBucketNotFound(t *testing.T) {
	b := setupTestBackend(t)

	_, _, err := b.GetObject("nope", "file.txt")
	require.ErrorIs(t, err, ErrBucketNotFound)
}

func TestHeadObject(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket("test-bucket")

	_, err := b.HeadObject("test-bucket", "nope.txt")
	require.ErrorIs(t, err, ErrObjectNotFound)

	data := []byte("head test")
	b.PutObject("test-bucket", "file.txt", bytes.NewReader(data), "application/octet-stream")

	obj, err := b.HeadObject("test-bucket", "file.txt")
	require.NoError(t, err, "HeadObject")
	assert.Equal(t, int64(len(data)), obj.Size)
}

func TestDeleteObject(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket("test-bucket")

	b.PutObject("test-bucket", "file.txt", bytes.NewReader([]byte("data")), "text/plain")

	require.NoError(t, b.DeleteObject("test-bucket", "file.txt"), "DeleteObject")

	_, err := b.HeadObject("test-bucket", "file.txt")
	require.ErrorIs(t, err, ErrObjectNotFound)

	// deleting nonexistent is not an error (S3 behavior)
	require.NoError(t, b.DeleteObject("test-bucket", "nonexistent"), "DeleteObject nonexistent should not error")
}

func TestListObjects(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket("test-bucket")

	b.PutObject("test-bucket", "docs/a.txt", bytes.NewReader([]byte("a")), "text/plain")
	b.PutObject("test-bucket", "docs/b.txt", bytes.NewReader([]byte("b")), "text/plain")
	b.PutObject("test-bucket", "images/c.png", bytes.NewReader([]byte("c")), "image/png")

	// list all
	objs, err := b.ListObjects("test-bucket", "", 1000)
	require.NoError(t, err, "ListObjects")
	require.Len(t, objs, 3)

	// list with prefix
	objs, err = b.ListObjects("test-bucket", "docs/", 1000)
	require.NoError(t, err, "ListObjects with prefix")
	require.Len(t, objs, 2)

	// list with maxKeys
	objs, err = b.ListObjects("test-bucket", "", 1)
	require.NoError(t, err, "ListObjects with maxKeys")
	require.Len(t, objs, 1)
}

func TestPutObjectOverwrite(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket("test-bucket")

	b.PutObject("test-bucket", "file.txt", bytes.NewReader([]byte("v1")), "text/plain")
	b.PutObject("test-bucket", "file.txt", bytes.NewReader([]byte("version2")), "text/plain")

	rc, meta, err := b.GetObject("test-bucket", "file.txt")
	require.NoError(t, err, "GetObject")
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	assert.Equal(t, "version2", string(got))
	assert.Equal(t, int64(8), meta.Size)
}

func TestPutObjectToBucketNotFound(t *testing.T) {
	b := setupTestBackend(t)

	_, err := b.PutObject("nope", "file.txt", bytes.NewReader([]byte("data")), "text/plain")
	require.ErrorIs(t, err, ErrBucketNotFound)
}

func TestLargeObject(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket("test-bucket")

	// 10MB object
	size := 10 * 1024 * 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}

	obj, err := b.PutObject("test-bucket", "large.bin", bytes.NewReader(data), "application/octet-stream")
	require.NoError(t, err, "PutObject large")
	assert.Equal(t, int64(size), obj.Size)

	rc, _, err := b.GetObject("test-bucket", "large.bin")
	require.NoError(t, err, "GetObject large")
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	assert.Equal(t, data, got)

	// verify file on disk
	_, err = os.Stat(b.objectPath("test-bucket", "large.bin"))
	require.NoError(t, err, "expected file on disk")
}

func TestLocalBackend_BucketPolicy(t *testing.T) {
	b := setupTestBackend(t)
	require.NoError(t, b.CreateBucket("policy-bucket"))

	// No policy initially
	_, err := b.GetBucketPolicy("policy-bucket")
	assert.ErrorIs(t, err, ErrBucketNotFound)

	// Set policy
	policy := []byte(`{"Version":"2012-10-17","Statement":[]}`)
	require.NoError(t, b.SetBucketPolicy("policy-bucket", policy))

	// Get policy
	got, err := b.GetBucketPolicy("policy-bucket")
	require.NoError(t, err)
	assert.Equal(t, policy, got)

	// Delete policy
	require.NoError(t, b.DeleteBucketPolicy("policy-bucket"))

	// Verify deleted
	_, err = b.GetBucketPolicy("policy-bucket")
	assert.ErrorIs(t, err, ErrBucketNotFound)

	// Delete non-existent policy (should not error)
	require.NoError(t, b.DeleteBucketPolicy("policy-bucket"))
}

func TestLocalBackend_Close(t *testing.T) {
	b := setupTestBackend(t)
	require.NoError(t, b.Close())
}

func TestLocalBackend_WriteAt(t *testing.T) {
	b := setupTestBackend(t)
	require.NoError(t, b.CreateBucket("bkt"))

	full := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ") // 26 bytes

	// Seed the file via PutObject.
	_, err := b.PutObject("bkt", "key", bytes.NewReader(full), "application/octet-stream")
	require.NoError(t, err)

	cases := []struct {
		name       string
		offset     uint64
		data       []byte
		wantBytes  []byte
		wantSize   int64
	}{
		{
			name:      "overwrite middle",
			offset:    5,
			data:      []byte("XYZ"),
			wantBytes: []byte("ABCDEXYZIJKLMNOPQRSTUVWXYZ"),
			wantSize:  26,
		},
		{
			name:      "overwrite and extend",
			offset:    24,
			data:      []byte("0123"),
			wantBytes: []byte("ABCDEXYZIJKLMNOPQRSTUVWX0123"),
			wantSize:  28,
		},
		{
			name:      "overwrite from zero",
			offset:    0,
			data:      []byte("aa"),
			wantBytes: []byte("aaCDEXYZIJKLMNOPQRSTUVWX0123"),
			wantSize:  28,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obj, err := b.WriteAt("bkt", "key", tc.offset, tc.data)
			require.NoError(t, err)
			assert.Equal(t, tc.wantSize, obj.Size)

			rc, got, err := b.GetObject("bkt", "key")
			require.NoError(t, err)
			defer rc.Close()
			gotBytes, _ := io.ReadAll(rc)
			assert.Equal(t, tc.wantBytes, gotBytes)
			assert.Equal(t, tc.wantSize, got.Size)
		})
	}
}

func TestLocalBackend_WriteAt_NewFile(t *testing.T) {
	b := setupTestBackend(t)
	require.NoError(t, b.CreateBucket("bkt"))

	// Write at offset > 0 to a non-existent file → should create with sparse prefix.
	obj, err := b.WriteAt("bkt", "sparse", 4, []byte("DATA"))
	require.NoError(t, err)
	assert.Equal(t, int64(8), obj.Size)

	rc, _, err := b.GetObject("bkt", "sparse")
	require.NoError(t, err)
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	assert.Equal(t, 8, len(got))
	assert.Equal(t, []byte{0, 0, 0, 0}, got[:4]) // sparse hole = zeros
	assert.Equal(t, []byte("DATA"), got[4:])
}

func TestCachedBackend_WriteAt(t *testing.T) {
	b := setupTestBackend(t)
	require.NoError(t, b.CreateBucket("bkt"))
	cached := NewCachedBackend(b)

	_, err := cached.PutObject("bkt", "key", bytes.NewReader([]byte("hello world")), "text/plain")
	require.NoError(t, err)

	// Warm the cache.
	rc, _, err := cached.GetObject("bkt", "key")
	require.NoError(t, err)
	rc.Close()

	// WriteAt should invalidate cache and update content.
	obj, err := cached.WriteAt("bkt", "key", 6, []byte("Go!"))
	require.NoError(t, err)
	assert.Equal(t, int64(11), obj.Size)

	rc, _, err = cached.GetObject("bkt", "key")
	require.NoError(t, err)
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	assert.Equal(t, []byte("hello Go!ld"), got)
}

func TestCachedBackend_CloseAndUnwrap(t *testing.T) {
	b := setupTestBackend(t)
	cached := NewCachedBackend(b)

	assert.Equal(t, b, cached.Unwrap())
	require.NoError(t, cached.Close())
}

func TestLocalBackend_ReadAt(t *testing.T) {
	b := setupTestBackend(t)
	require.NoError(t, b.CreateBucket("bkt"))

	data := []byte("hello world")
	_, err := b.PutObject("bkt", "obj", bytes.NewReader(data), "application/octet-stream")
	require.NoError(t, err)

	buf := make([]byte, 5)
	n, err := b.ReadAt("bkt", "obj", 6, buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("world"), buf[:n])
}

func TestLocalBackend_ReadAt_EOF(t *testing.T) {
	b := setupTestBackend(t)
	require.NoError(t, b.CreateBucket("bkt"))

	_, err := b.PutObject("bkt", "obj", bytes.NewReader([]byte("abc")), "application/octet-stream")
	require.NoError(t, err)

	buf := make([]byte, 10)
	n, err := b.ReadAt("bkt", "obj", 0, buf)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 3, n)
	assert.Equal(t, []byte("abc"), buf[:n])
}

func TestLocalBackend_ReadAt_NotExist(t *testing.T) {
	b := setupTestBackend(t)
	require.NoError(t, b.CreateBucket("bkt"))

	buf := make([]byte, 4)
	_, err := b.ReadAt("bkt", "missing", 0, buf)
	assert.True(t, os.IsNotExist(err))
}

func TestCachedBackend_ReadAt_CacheHit(t *testing.T) {
	cb, _ := newTestCachedBackend(t, WithMaxObjectCacheBytes(1024*1024))
	require.NoError(t, cb.CreateBucket("bkt"))

	data := []byte("cached content")
	_, err := cb.PutObject("bkt", "obj", bytes.NewReader(data), "application/octet-stream")
	require.NoError(t, err)

	// Warm the cache.
	rc, _, err := cb.GetObject("bkt", "obj")
	require.NoError(t, err)
	rc.Close()

	buf := make([]byte, 6)
	n, err := cb.ReadAt("bkt", "obj", 7, buf)
	require.NoError(t, err)
	assert.Equal(t, 6, n)
	assert.Equal(t, []byte("conten"), buf[:n])
}

func TestCachedBackend_ReadAt_CacheMiss(t *testing.T) {
	cb, _ := newTestCachedBackend(t)
	require.NoError(t, cb.CreateBucket("bkt"))

	data := []byte("uncached data")
	_, err := cb.PutObject("bkt", "obj", bytes.NewReader(data), "application/octet-stream")
	require.NoError(t, err)

	buf := make([]byte, 8)
	n, err := cb.ReadAt("bkt", "obj", 0, buf)
	require.NoError(t, err)
	assert.Equal(t, 8, n)
	assert.Equal(t, []byte("uncached"), buf[:n])
}
