package packblob

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPackedBackend_CloseCallsSaveIndex verifies that Close() persists the index
// including refcount metadata, so CopyObject refcounts survive restart.
// Without SaveIndex, LoadIndex rebuilds from blob files with refcount=1,
// causing data corruption when objects have shared references.
func TestPackedBackend_CloseCallsSaveIndex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "packblob-close-saves-index")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	inner := &mockBackend{}
	pb, err := NewPackedBackend(inner, tmpDir, 1024)
	require.NoError(t, err)

	_, err = pb.PutObject("bucket", "key1", bytes.NewReader([]byte("hello")), "text/plain")
	require.NoError(t, err)

	// CopyObject increments refcount to 2
	_, err = pb.CopyObject("bucket", "key1", "bucket", "key2")
	require.NoError(t, err)

	pb.mu.RLock()
	entry := pb.index["bucket/key1"]
	refcountBefore := entry.Refcount.Load()
	pb.mu.RUnlock()
	require.Equal(t, int64(2), refcountBefore, "refcount must be 2 after copy")

	// Close without explicitly calling SaveIndex — this is the bug scenario.
	// If Close() doesn't call SaveIndex(), index.json is never written.
	require.NoError(t, pb.Close())

	// Simulate restart
	pb2, err := NewPackedBackend(inner, tmpDir, 1024)
	require.NoError(t, err)
	defer pb2.Close()

	require.NoError(t, pb2.LoadIndex())

	// Refcount must be 2 — proves Close() saved the index (not just rebuilt from blobs)
	pb2.mu.RLock()
	entry2 := pb2.index["bucket/key1"]
	refcountAfter := entry2.Refcount.Load()
	pb2.mu.RUnlock()

	assert.Equal(t, int64(2), refcountAfter,
		"refcount must survive restart: Close() must call SaveIndex()")
}
