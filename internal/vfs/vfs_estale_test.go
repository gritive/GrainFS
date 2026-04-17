package vfs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestVFSESTALESupport verifies MarkDeleted and IsDeleted methods.
func TestVFSESTALESupport(t *testing.T) {
	backend := &mockBackend{
		data: make(map[string]string),
	}

	fs, err := New(backend, "test-volume",
		WithStatCacheTTL(10*time.Second)) // Enable stat cache for deleted tracking
	assert.NoError(t, err)

	// Mark a file as deleted
	err = fs.MarkDeleted("test-bucket", "file.txt")
	assert.NoError(t, err)

	// Verify it's marked as deleted
	assert.True(t, fs.IsDeleted("test-bucket", "file.txt"))

	// Different file should not be marked
	assert.False(t, fs.IsDeleted("test-bucket", "other.txt"))

	// Different bucket should not be marked
	assert.False(t, fs.IsDeleted("other-bucket", "file.txt"))
}

// TestVFSESTALEWithRealDelete simulates real workflow: delete via backend, then mark deleted.
func TestVFSESTALEWithRealDelete(t *testing.T) {
	backend := &mockBackend{
		data: make(map[string]string),
	}

	fs, err := New(backend, "test-volume",
		WithStatCacheTTL(10*time.Second))
	assert.NoError(t, err)

	// Create file
	backend.data["__grainfs_vfs_test-volume/file.txt"] = "content"

	// Stat it (populates cache)
	info1, err := fs.Stat("file.txt")
	assert.NoError(t, err)
	assert.NotNil(t, info1)

	// Delete file via backend (simulates S3 DELETE)
	delete(backend.data, "__grainfs_vfs_test-volume/file.txt")

	// Mark as deleted (should be called by OnApply callback)
	err = fs.MarkDeleted("test-volume", "file.txt")
	assert.NoError(t, err)

	// Verify it's marked
	assert.True(t, fs.IsDeleted("test-volume", "file.txt"))

	// Future Stat calls should check IsDeleted (NFS handler responsibility)
	isDeleted := fs.IsDeleted("test-volume", "file.txt")
	assert.True(t, isDeleted, "file should remain marked as deleted")
}
