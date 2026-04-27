package vfs

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/assert"
)

// TestVFSInvalidate verifies that Invalidate() correctly clears stat/dir caches.
func TestVFSInvalidate(t *testing.T) {
	// Create in-memory backend
	backend := &mockBackend{
		data: make(map[string]string),
	}

	// Create VFS with short cache TTLs for testing
	fs, err := New(backend, "test-volume",
		WithStatCacheTTL(10*time.Second),
		WithDirCacheTTL(10*time.Second))
	assert.NoError(t, err)

	// Create a file via backend (simulating S3 PUT)
	backend.data["__grainfs_vfs_test-volume/file.txt"] = "hello"

	// Stat the file via VFS (should cache the result)
	info1, err := fs.Stat("file.txt")
	assert.NoError(t, err)
	assert.NotNil(t, info1)

	// Verify cache hit by checking stat cache directly
	m := fs.statCache.Load()
	_, cached := (*m)["file.txt"]
	assert.True(t, cached, "stat cache should be populated after Stat()")

	// Call Invalidate (simulating Raft OnApply callback)
	fs.Invalidate("test-volume", "file.txt")

	// Verify stat cache was cleared
	m2 := fs.statCache.Load()
	_, stillCached := (*m2)["file.txt"]
	assert.False(t, stillCached, "stat cache should be cleared after Invalidate()")
}

// TestVFSInvalidateDifferentBucket verifies that Invalidate() only affects matching buckets.
func TestVFSInvalidateDifferentBucket(t *testing.T) {
	backend := &mockBackend{
		data: make(map[string]string),
	}

	fs, err := New(backend, "test-volume",
		WithStatCacheTTL(10*time.Second))
	assert.NoError(t, err)

	// Create a file
	backend.data["__grainfs_vfs_test-volume/file.txt"] = "hello"

	// Stat to populate cache
	info1, err := fs.Stat("file.txt")
	assert.NoError(t, err)
	assert.NotNil(t, info1)

	// Call Invalidate for different bucket (should be no-op)
	fs.Invalidate("other-volume", "file.txt")

	// Verify cache still present
	m := fs.statCache.Load()
	cached := (*m)["file.txt"]
	assert.NotNil(t, cached, "stat cache should NOT be cleared for different bucket")
}

// mockBackend is a simple in-memory storage backend for testing.
type mockBackend struct {
	data map[string]string
}

func (m *mockBackend) CreateBucket(bucket string) error {
	m.data[bucket] = ""
	return nil
}

func (m *mockBackend) HeadBucket(bucket string) error {
	if _, ok := m.data[bucket]; ok {
		return nil
	}
	return storage.ErrBucketNotFound
}

func (m *mockBackend) DeleteBucket(bucket string) error {
	delete(m.data, bucket)
	return nil
}

func (m *mockBackend) ListBuckets() ([]string, error) {
	var buckets []string
	for k := range m.data {
		if len(k) == 0 || k[len(k)-1] != '/' {
			buckets = append(buckets, k)
		}
	}
	return buckets, nil
}

func (m *mockBackend) PutObject(bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	return nil, nil
}

func (m *mockBackend) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	return nil, nil, nil
}

func (m *mockBackend) HeadObject(bucket, key string) (*storage.Object, error) {
	fullKey := bucket + "/" + key
	if _, ok := m.data[fullKey]; ok {
		return &storage.Object{Key: key, Size: 100}, nil
	}
	return nil, storage.ErrObjectNotFound
}

func (m *mockBackend) DeleteObject(bucket, key string) error {
	delete(m.data, bucket+"/"+key)
	return nil
}

func (m *mockBackend) ListObjects(bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	return nil, errors.New("not implemented")
}

func (m *mockBackend) WalkObjects(bucket, prefix string, fn func(*storage.Object) error) error {
	return errors.New("not implemented")
}

func (m *mockBackend) CreateMultipartUpload(bucket, key, contentType string) (*storage.MultipartUpload, error) {
	return nil, errors.New("not implemented")
}

func (m *mockBackend) UploadPart(bucket, key, uploadID string, partNumber int, r io.Reader) (*storage.Part, error) {
	return nil, errors.New("not implemented")
}

func (m *mockBackend) CompleteMultipartUpload(bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	return nil, errors.New("not implemented")
}

func (m *mockBackend) AbortMultipartUpload(bucket, key, uploadID string) error {
	return errors.New("not implemented")
}
