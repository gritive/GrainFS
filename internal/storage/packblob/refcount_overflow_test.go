package packblob

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCopyObject_RefcountOverflow tests that CopyObject detects and prevents refcount overflow
func TestCopyObject_RefcountOverflow(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "refcount-overflow-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	inner := &mockBackend{}
	pb, err := NewPackedBackend(inner, tmpDir, 1024)
	require.NoError(t, err)

	// Write initial object
	data := []byte("test data for refcount overflow")
	_, err = pb.PutObject("bucket1", "key1", bytes.NewReader(data), "text/plain")
	require.NoError(t, err)

	// Get the index entry and manually set refcount to near max int64
	ikey := pb.indexKey("bucket1", "key1")
	pb.mu.Lock()
	entry, ok := pb.index[ikey]
	require.True(t, ok, "key1 should exist in index")
	// Set refcount to max int64 - 1 (next increment would overflow)
	entry.Refcount.Store(9223372036854775806) // int64 max - 1
	pb.mu.Unlock()

	// TEST: CopyObject should detect potential overflow and return error
	result, err := pb.CopyObject("bucket1", "key1", "bucket1", "key2")
	if err == nil {
		t.Logf("CopyObject unexpectedly succeeded: result = %+v", result)
		// Check refcount after copy
		pb.mu.RLock()
		entryAfter, okAfter := pb.index[ikey]
		if okAfter {
			t.Logf("Refcount after copy: %d", entryAfter.Refcount.Load())
		}
		pb.mu.RUnlock()
	}
	assert.Error(t, err, "CopyObject should return error when refcount would overflow")
	if err != nil {
		assert.Contains(t, err.Error(), "overflow", "Error message should mention overflow")
	}

	// TEST: Verify refcount was NOT incremented
	pb.mu.RLock()
	entryAfter, okAfter := pb.index[ikey]
	require.True(t, okAfter, "key1 should still exist")
	finalRefcount := entryAfter.Refcount.Load()
	pb.mu.RUnlock()
	assert.Equal(t, int64(9223372036854775806), finalRefcount, "Refcount should not be incremented when overflow detected")
}

// TestCopyObject_RefcountAtMaxValue tests edge case at exact max int64
func TestCopyObject_RefcountAtMaxValue(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "refcount-max-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	inner := &mockBackend{}
	pb, err := NewPackedBackend(inner, tmpDir, 1024)
	require.NoError(t, err)

	// Write initial object
	data := []byte("test data for refcount at max")
	_, err = pb.PutObject("bucket1", "key1", bytes.NewReader(data), "text/plain")
	require.NoError(t, err)

	// Get the index entry and set refcount to max int64
	ikey := pb.indexKey("bucket1", "key1")
	pb.mu.Lock()
	entry, ok := pb.index[ikey]
	require.True(t, ok, "key1 should exist in index")
	entry.Refcount.Store(9223372036854775807) // int64 max
	pb.mu.Unlock()

	// TEST: CopyObject should detect overflow and return error
	_, err = pb.CopyObject("bucket1", "key1", "bucket1", "key2")
	assert.Error(t, err, "CopyObject should return error when refcount is at max")
	assert.Contains(t, err.Error(), "overflow", "Error message should mention overflow")

	// TEST: Verify refcount was NOT incremented
	pb.mu.RLock()
	entryAfter, okAfter := pb.index[ikey]
	require.True(t, okAfter, "key1 should still exist")
	finalRefcount := entryAfter.Refcount.Load()
	pb.mu.RUnlock()
	assert.Equal(t, int64(9223372036854775807), finalRefcount, "Refcount should remain at max when overflow detected")
}

// TestCopyObject_NormalRefcountIncrement tests normal refcount behavior
func TestCopyObject_NormalRefcountIncrement(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "refcount-normal-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	inner := &mockBackend{}
	pb, err := NewPackedBackend(inner, tmpDir, 1024)
	require.NoError(t, err)

	// Write initial object
	data := []byte("test data for normal refcount")
	_, err = pb.PutObject("bucket1", "key1", bytes.NewReader(data), "text/plain")
	require.NoError(t, err)

	// Copy object (should succeed normally)
	_, err = pb.CopyObject("bucket1", "key1", "bucket1", "key2")
	require.NoError(t, err, "CopyObject should succeed for normal refcount")

	// Verify refcount incremented to 2
	ikey := pb.indexKey("bucket1", "key1")
	pb.mu.RLock()
	entry := pb.index[ikey]
	refcount := entry.Refcount.Load()
	pb.mu.RUnlock()
	assert.Equal(t, int64(2), refcount, "Refcount should be 2 after one copy")
}
