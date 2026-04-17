package vfs

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestCrossProtocolDeleteESTALE simulates S3 DELETE → NFS ESTALE flow.
func TestCrossProtocolDeleteESTALE(t *testing.T) {
	backend := &mockBackend{
		data: make(map[string]string),
	}

	fs, err := New(backend, "test-volume",
		WithStatCacheTTL(10*time.Second))
	assert.NoError(t, err)

	// Create file via backend (simulates S3 PUT)
	backend.data["__grainfs_vfs_test-volume/file.txt"] = "content"

	// Stat via VFS (should succeed)
	info1, err := fs.Stat("file.txt")
	assert.NoError(t, err)
	assert.NotNil(t, info1)
	assert.Equal(t, "file.txt", info1.Name())

	// Delete file via backend (simulates S3 DELETE)
	delete(backend.data, "__grainfs_vfs_test-volume/file.txt")

	// Mark as deleted (simulates Raft OnApply callback)
	err = fs.MarkDeleted("test-volume", "file.txt")
	assert.NoError(t, err)

	// Stat via VFS again (should return ErrNotExist = ESTALE for NFS)
	info2, err := fs.Stat("file.txt")
	assert.Error(t, err)
	assert.Nil(t, info2)
	assert.Equal(t, os.ErrNotExist, err, "Stat should return ErrNotExist for deleted files")
}

// TestCrossProtocolDeleteInvalidate verifies that Invalidate also clears deleted marker.
// When a file is deleted and then re-created, the deleted marker should be cleared.
func TestCrossProtocolDeleteInvalidate(t *testing.T) {
	backend := &mockBackend{
		data: make(map[string]string),
	}

	fs, err := New(backend, "test-volume",
		WithStatCacheTTL(10*time.Second))
	assert.NoError(t, err)

	// Create file
	backend.data["__grainfs_vfs_test-volume/file.txt"] = "content"

	// Stat it
	info1, _ := fs.Stat("file.txt")
	assert.NotNil(t, info1)

	// Delete and mark
	delete(backend.data, "__grainfs_vfs_test-volume/file.txt")
	fs.MarkDeleted("test-volume", "file.txt")

	// Verify it's deleted
	assert.True(t, fs.IsDeleted("test-volume", "file.txt"))

	// Re-create file (simulates S3 PUT after DELETE)
	backend.data["__grainfs_vfs_test-volume/file.txt"] = "new content"

	// Invalidate cache (simulates Raft OnApply after PUT)
	fs.Invalidate("test-volume", "file.txt")

	// Stat should succeed again (file re-created)
	info2, err := fs.Stat("file.txt")
	assert.NoError(t, err)
	assert.NotNil(t, info2)
}
