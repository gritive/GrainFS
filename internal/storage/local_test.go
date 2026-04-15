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

func TestCachedBackend_CloseAndUnwrap(t *testing.T) {
	b := setupTestBackend(t)
	cached := NewCachedBackend(b)

	assert.Equal(t, b, cached.Unwrap())
	require.NoError(t, cached.Close())
}
