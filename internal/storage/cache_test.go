package storage

import (
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestCachedBackend(t *testing.T, opts ...CacheOption) (*CachedBackend, *LocalBackend) {
	t.Helper()
	dir := t.TempDir()
	backend, err := NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	cached := NewCachedBackend(backend, opts...)
	return cached, backend
}

func TestCachedBackend_GetObjectCacheHit(t *testing.T) {
	cb, _ := newTestCachedBackend(t)

	require.NoError(t, cb.CreateBucket("test"))

	data := "hello cache"
	_, err := cb.PutObject("test", "key1", strings.NewReader(data), "text/plain")
	require.NoError(t, err)

	// First read: cache miss
	rc1, obj1, err := cb.GetObject("test", "key1")
	require.NoError(t, err)
	body1, _ := io.ReadAll(rc1)
	rc1.Close()
	assert.Equal(t, data, string(body1))
	assert.Equal(t, int64(len(data)), obj1.Size)

	// Second read: cache hit — should return identical data
	rc2, obj2, err := cb.GetObject("test", "key1")
	require.NoError(t, err)
	body2, _ := io.ReadAll(rc2)
	rc2.Close()
	assert.Equal(t, data, string(body2))
	assert.Equal(t, obj1.ETag, obj2.ETag)

	// Verify stats
	assert.Equal(t, int64(1), cb.Stats().Misses)
	assert.Equal(t, int64(1), cb.Stats().Hits)
}

func TestCachedBackend_HeadObjectCacheHit(t *testing.T) {
	cb, _ := newTestCachedBackend(t)

	require.NoError(t, cb.CreateBucket("test"))
	_, err := cb.PutObject("test", "key1", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)

	// First: cache miss
	obj1, err := cb.HeadObject("test", "key1")
	require.NoError(t, err)
	assert.Equal(t, int64(4), obj1.Size)

	// Second: cache hit
	obj2, err := cb.HeadObject("test", "key1")
	require.NoError(t, err)
	assert.Equal(t, obj1.ETag, obj2.ETag)
}

func TestCachedBackend_InvalidateOnPut(t *testing.T) {
	cb, _ := newTestCachedBackend(t)

	require.NoError(t, cb.CreateBucket("test"))
	_, err := cb.PutObject("test", "key1", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)

	// Populate cache
	rc, _, err := cb.GetObject("test", "key1")
	require.NoError(t, err)
	rc.Close()

	// Overwrite
	_, err = cb.PutObject("test", "key1", strings.NewReader("v2"), "text/plain")
	require.NoError(t, err)

	// Read should get v2, not cached v1
	rc, _, err = cb.GetObject("test", "key1")
	require.NoError(t, err)
	body, _ := io.ReadAll(rc)
	rc.Close()
	assert.Equal(t, "v2", string(body))
}

func TestCachedBackend_InvalidateOnDelete(t *testing.T) {
	cb, _ := newTestCachedBackend(t)

	require.NoError(t, cb.CreateBucket("test"))
	_, err := cb.PutObject("test", "key1", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)

	// Populate cache
	rc, _, err := cb.GetObject("test", "key1")
	require.NoError(t, err)
	rc.Close()

	// Delete
	require.NoError(t, cb.DeleteObject("test", "key1"))

	// Read should fail
	_, _, err = cb.GetObject("test", "key1")
	assert.Error(t, err)
}

func TestCachedBackend_InvalidateOnMultipartComplete(t *testing.T) {
	cb, _ := newTestCachedBackend(t)

	require.NoError(t, cb.CreateBucket("test"))

	// Put initial version and cache it
	_, err := cb.PutObject("test", "mp-key", strings.NewReader("original"), "text/plain")
	require.NoError(t, err)
	rc, _, err := cb.GetObject("test", "mp-key")
	require.NoError(t, err)
	rc.Close()

	// Do multipart upload
	mp, err := cb.CreateMultipartUpload("test", "mp-key", "text/plain")
	require.NoError(t, err)

	part, err := cb.UploadPart("test", "mp-key", mp.UploadID, 1, strings.NewReader("multipart-data"))
	require.NoError(t, err)

	_, err = cb.CompleteMultipartUpload("test", "mp-key", mp.UploadID, []Part{*part})
	require.NoError(t, err)

	// Read should get multipart data, not cached original
	rc, _, err = cb.GetObject("test", "mp-key")
	require.NoError(t, err)
	body, _ := io.ReadAll(rc)
	rc.Close()
	assert.Equal(t, "multipart-data", string(body))
}

func TestCachedBackend_EvictionOnSizeLimit(t *testing.T) {
	// Cache limit: 50 bytes
	cb, _ := newTestCachedBackend(t, WithMaxCacheBytes(50))

	require.NoError(t, cb.CreateBucket("test"))

	// Put 3 objects of 20 bytes each (total 60 > 50)
	for _, key := range []string{"a", "b", "c"} {
		_, err := cb.PutObject("test", key, strings.NewReader(strings.Repeat("x", 20)), "text/plain")
		require.NoError(t, err)
		rc, _, err := cb.GetObject("test", key)
		require.NoError(t, err)
		rc.Close()
	}

	// At least one entry should have been evicted
	stats := cb.Stats()
	assert.True(t, stats.Evictions > 0, "expected evictions when cache is full")
}

func TestCachedBackend_ConcurrentAccess(t *testing.T) {
	cb, _ := newTestCachedBackend(t)

	require.NoError(t, cb.CreateBucket("test"))
	_, err := cb.PutObject("test", "shared", strings.NewReader("concurrent"), "text/plain")
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rc, _, err := cb.GetObject("test", "shared")
			if err != nil {
				return
			}
			body, _ := io.ReadAll(rc)
			rc.Close()
			assert.Equal(t, "concurrent", string(body))
		}()
	}
	wg.Wait()
}

func TestCachedBackend_LargeObjectNotCached(t *testing.T) {
	// Max single object size: 10 bytes
	cb, _ := newTestCachedBackend(t, WithMaxObjectCacheBytes(10))

	require.NoError(t, cb.CreateBucket("test"))

	// Object larger than per-object limit
	large := strings.Repeat("x", 100)
	_, err := cb.PutObject("test", "big", strings.NewReader(large), "text/plain")
	require.NoError(t, err)

	rc1, _, err := cb.GetObject("test", "big")
	require.NoError(t, err)
	body1, _ := io.ReadAll(rc1)
	rc1.Close()
	assert.Equal(t, large, string(body1))

	// Second read is still a miss because object was too large to cache
	rc2, _, err := cb.GetObject("test", "big")
	require.NoError(t, err)
	body2, _ := io.ReadAll(rc2)
	rc2.Close()
	assert.Equal(t, large, string(body2))

	assert.Equal(t, int64(0), cb.Stats().Hits)
}

func TestCachedBackend_PassthroughBucketOps(t *testing.T) {
	cb, _ := newTestCachedBackend(t)

	// All bucket operations pass through correctly
	require.NoError(t, cb.CreateBucket("b1"))
	require.NoError(t, cb.HeadBucket("b1"))

	buckets, err := cb.ListBuckets()
	require.NoError(t, err)
	assert.Contains(t, buckets, "b1")

	require.NoError(t, cb.DeleteBucket("b1"))
	assert.Error(t, cb.HeadBucket("b1"))
}

func TestCachedBackend_PassthroughListObjects(t *testing.T) {
	cb, _ := newTestCachedBackend(t)

	require.NoError(t, cb.CreateBucket("test"))
	_, err := cb.PutObject("test", "a", strings.NewReader("1"), "text/plain")
	require.NoError(t, err)
	_, err = cb.PutObject("test", "b", strings.NewReader("2"), "text/plain")
	require.NoError(t, err)

	objs, err := cb.ListObjects("test", "", 100)
	require.NoError(t, err)
	assert.Len(t, objs, 2)
}

func TestCachedBackend_GetObjectReturnsIndependentReaders(t *testing.T) {
	cb, _ := newTestCachedBackend(t)

	require.NoError(t, cb.CreateBucket("test"))
	_, err := cb.PutObject("test", "k", strings.NewReader("abcdef"), "text/plain")
	require.NoError(t, err)

	// Populate cache
	rc, _, _ := cb.GetObject("test", "k")
	io.ReadAll(rc)
	rc.Close()

	// Get two readers from cache
	r1, _, _ := cb.GetObject("test", "k")
	r2, _, _ := cb.GetObject("test", "k")

	// Read partial from r1
	buf := make([]byte, 3)
	_, _ = r1.Read(buf)

	// r2 should still read full content
	body2, _ := io.ReadAll(r2)
	assert.Equal(t, "abcdef", string(body2))

	rest, _ := io.ReadAll(r1)
	assert.Equal(t, "def", string(rest))

	r1.Close()
	r2.Close()
}

func TestCachedBackend_AbortMultipartPassthrough(t *testing.T) {
	cb, _ := newTestCachedBackend(t)

	require.NoError(t, cb.CreateBucket("test"))
	_, err := cb.PutObject("test", "stays", strings.NewReader("persistent"), "text/plain")
	require.NoError(t, err)

	// Populate cache
	rc, _, _ := cb.GetObject("test", "stays")
	io.ReadAll(rc)
	rc.Close()

	// Start and abort multipart
	mp, err := cb.CreateMultipartUpload("test", "stays", "text/plain")
	require.NoError(t, err)
	require.NoError(t, cb.AbortMultipartUpload("test", "stays", mp.UploadID))

	// Cache should still be valid
	rc, _, err = cb.GetObject("test", "stays")
	require.NoError(t, err)
	body, _ := io.ReadAll(rc)
	rc.Close()
	assert.Equal(t, "persistent", string(body))
}
