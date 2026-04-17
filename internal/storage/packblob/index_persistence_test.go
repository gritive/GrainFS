package packblob

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestIndex_SaveAndLoad tests that index can be saved and loaded
func TestIndex_SaveAndLoad(t *testing.T) {
	// Setup: Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "packed-index-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create PackedBackend with temp directory
	inner := &mockBackend{}
	pb, err := NewPackedBackend(inner, tmpDir, 1024)
	require.NoError(t, err)

	// Write test data
	pb.mu.Lock()
	pb.index["bucket1/key1"] = &indexEntry{
		Location: BlobLocation{BlobID: 1, Offset: 100, Length: 200},
	}
	pb.index["bucket1/key2"] = &indexEntry{
		Location: BlobLocation{BlobID: 1, Offset: 300, Length: 400},
	}
	pb.index["bucket2/key1"] = &indexEntry{
		Location: BlobLocation{BlobID: 2, Offset: 100, Length: 200},
	}
	pb.mu.Unlock()

	// TEST: Save index
	err = pb.SaveIndex()
	assert.NoError(t, err, "SaveIndex should succeed")

	// TEST: Verify index file exists
	indexFile := filepath.Join(tmpDir, "index.json")
	_, err = os.Stat(indexFile)
	assert.NoError(t, err, "Index file should exist")

	require.NoError(t, pb.Close())

	// Create new PackedBackend to simulate restart
	pb2, err := NewPackedBackend(inner, tmpDir, 1024)
	require.NoError(t, err)
	defer pb2.Close()

	// TEST: Load index
	err = pb2.LoadIndex()
	assert.NoError(t, err, "LoadIndex should succeed")

	// TEST: Verify index restored
	pb2.mu.RLock()
	assert.Len(t, pb2.index, 3, "Should have 3 entries in restored index")

	// Verify specific entries
	entry1, ok := pb2.index["bucket1/key1"]
	assert.True(t, ok, "bucket1/key1 should exist")
	assert.Equal(t, BlobLocation{BlobID: 1, Offset: 100, Length: 200}, entry1.Location)

	entry2, ok := pb2.index["bucket1/key2"]
	assert.True(t, ok, "bucket1/key2 should exist")
	assert.Equal(t, BlobLocation{BlobID: 1, Offset: 300, Length: 400}, entry2.Location)
	pb2.mu.RUnlock()
}

// TestIndex_LoadRebuildFromBlobs tests index rebuild from blob files
func TestIndex_LoadRebuildFromBlobs(t *testing.T) {
	// Setup: Create temporary directory
	tmpDir, err := os.MkdirTemp("", "packed-index-rebuild-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create PackedBackend and write test data
	inner := &mockBackend{}
	pb, err := NewPackedBackend(inner, tmpDir, 1024)
	require.NoError(t, err)

	// Write objects (this creates blob files)
	data1 := []byte("test data 1")
	_, err = pb.PutObject("bucket1", "key1", bytes.NewReader(data1), "text/plain")
	require.NoError(t, err)

	data2 := []byte("test data 2")
	_, err = pb.PutObject("bucket2", "key2", bytes.NewReader(data2), "text/plain")
	require.NoError(t, err)

	// Verify objects are in index
	pb.mu.RLock()
	initialCount := len(pb.index)
	pb.mu.RUnlock()
	assert.Greater(t, initialCount, 0, "Should have objects in index")

	// TEST: Simulate restart by creating new PackedBackend
	require.NoError(t, pb.Close())
	pb2, err := NewPackedBackend(inner, tmpDir, 1024)
	require.NoError(t, err)
	defer pb2.Close()

	// TEST: Load index (should rebuild from blob files)
	err = pb2.LoadIndex()
	assert.NoError(t, err, "LoadIndex should rebuild from blob files")

	// TEST: Verify all objects accessible after rebuild
	rc1, obj1, err := pb2.GetObject("bucket1", "key1")
	require.NoError(t, err)
	require.NotNil(t, rc1)
	defer rc1.Close()
	assert.Equal(t, "key1", obj1.Key)

	rc2, obj2, err := pb2.GetObject("bucket2", "key2")
	require.NoError(t, err)
	require.NotNil(t, rc2)
	defer rc2.Close()
	assert.Equal(t, "key2", obj2.Key)
}

// TestIndex_RebuildAfterCrash tests index rebuild after simulated crash
func TestIndex_RebuildAfterCrash(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "packed-index-crash-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	inner := &mockBackend{}
	pb, err := NewPackedBackend(inner, tmpDir, 1024)
	require.NoError(t, err)

	// Write 100 objects
	const objectCount = 100
	for i := 0; i < objectCount; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("test data %d", i))
		_, err := pb.PutObject("bucket1", key, bytes.NewReader(data), "text/plain")
		require.NoError(t, err)
	}

	// Save index
	err = pb.SaveIndex()
	require.NoError(t, err)

	// Simulate crash by deleting index file
	indexFile := filepath.Join(tmpDir, "index.json")
	err = os.Remove(indexFile)
	require.NoError(t, err)

	// Create new PackedBackend (simulates restart)
	require.NoError(t, pb.Close())
	pb2, err := NewPackedBackend(inner, tmpDir, 1024)
	require.NoError(t, err)
	defer pb2.Close()

	// TEST: LoadIndex should rebuild from blob files even without index file
	err = pb2.LoadIndex()
	assert.NoError(t, err, "LoadIndex should rebuild from blob files")

	// TEST: Verify all objects accessible
	for i := 0; i < objectCount; i++ {
		key := fmt.Sprintf("key%d", i)
		rc, obj, err := pb2.GetObject("bucket1", key)
		assert.NoError(t, err, "Object %s should be accessible after rebuild", key)
		assert.NotNil(t, rc)
		assert.NotNil(t, obj)
		rc.Close()
	}
}

// TestIndex_PersistenceWithRefcounts tests that refcounts are persisted
func TestIndex_PersistenceWithRefcounts(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "packed-index-refcount-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	inner := &mockBackend{}
	pb, err := NewPackedBackend(inner, tmpDir, 1024)
	require.NoError(t, err)

	// Write an object
	data := []byte("test data")
	_, err = pb.PutObject("bucket1", "key1", bytes.NewReader(data), "text/plain")
	require.NoError(t, err)

	// Copy the object (increments refcount)
	_, err = pb.CopyObject("bucket1", "key1", "bucket1", "key2")
	require.NoError(t, err)

	// Verify refcount is 2
	pb.mu.RLock()
	entry := pb.index["bucket1/key1"]
	refcountBefore := entry.Refcount.Load()
	pb.mu.RUnlock()
	assert.Equal(t, int64(2), refcountBefore, "Refcount should be 2 after copy")

	// TEST: Save and load index
	err = pb.SaveIndex()
	require.NoError(t, err)

	require.NoError(t, pb.Close())
	pb2, err := NewPackedBackend(inner, tmpDir, 1024)
	require.NoError(t, err)
	defer pb2.Close()
	err = pb2.LoadIndex()
	require.NoError(t, err)

	// TEST: Verify refcount persisted
	pb2.mu.RLock()
	entry2 := pb2.index["bucket1/key1"]
	refcountAfter := entry2.Refcount.Load()
	pb2.mu.RUnlock()
	assert.Equal(t, int64(2), refcountAfter, "Refcount should persist across restart")
}

// mockBackend is a minimal mock of storage.Backend for testing
type mockBackend struct {
	storage.Backend
}

func (m *mockBackend) CreateBucket(bucket string) error {
	return nil
}

func (m *mockBackend) HeadBucket(bucket string) error {
	return nil
}

func (m *mockBackend) DeleteBucket(bucket string) error {
	return nil
}

func (m *mockBackend) ListBuckets() ([]string, error) {
	return []string{}, nil
}

func (m *mockBackend) PutObject(bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	return &storage.Object{
		Key:          key,
		Size:         0,
		ContentType:  contentType,
		ETag:         "mock-etag",
		LastModified: 0,
	}, nil
}

func (m *mockBackend) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	return io.NopCloser(bytes.NewReader(nil)), &storage.Object{
		Key: key,
	}, nil
}

func (m *mockBackend) HeadObject(bucket, key string) (*storage.Object, error) {
	return &storage.Object{
		Key: key,
	}, nil
}

func (m *mockBackend) DeleteObject(bucket, key string) error {
	return nil
}

func (m *mockBackend) ListObjects(bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	return []*storage.Object{}, nil
}

func (m *mockBackend) Close() error {
	return nil
}

func (m *mockBackend) Unwrap() storage.Backend {
	return m
}
