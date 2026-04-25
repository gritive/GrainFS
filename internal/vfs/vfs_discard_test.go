package vfs

import (
	"os"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupFSWithVolume(t *testing.T) (*GrainVFS, *volume.Manager) {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)

	volMgr := volume.NewManager(backend)
	const volName = "test-vol"
	_, err = volMgr.Create(volName, 10*1024*1024)
	require.NoError(t, err)

	// Mark as tracking (simulate a write so AllocatedBlocks >= 0)
	_, err = volMgr.WriteAt(volName, make([]byte, 4096), 0)
	require.NoError(t, err)

	fs, err := New(backend, volName, WithVolumeManager(volMgr, volName))
	require.NoError(t, err)
	return fs, volMgr
}

// TestVFSDiscard verifies that Remove() decrements AllocatedBlocks.
func TestVFSDiscard(t *testing.T) {
	fs, volMgr := setupFSWithVolume(t)

	// Write a file
	f, err := fs.Create("big.txt")
	require.NoError(t, err)
	payload := make([]byte, 8192)
	_, err = f.Write(payload)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	vol, err := volMgr.Get("test-vol")
	require.NoError(t, err)
	beforeBlocks := vol.AllocatedBlocks

	require.NoError(t, fs.Remove("big.txt"))

	vol, err = volMgr.Get("test-vol")
	require.NoError(t, err)
	assert.Less(t, vol.AllocatedBlocks, beforeBlocks, "AllocatedBlocks should decrease after Remove")
}

// TestVFSTruncateDiscard verifies that truncating a file to a smaller size
// via Truncate()+Close() decrements AllocatedBlocks.
func TestVFSTruncateDiscard(t *testing.T) {
	fs, volMgr := setupFSWithVolume(t)

	// Write a large file
	f, err := fs.Create("shrink.txt")
	require.NoError(t, err)
	payload := make([]byte, 8192)
	_, err = f.Write(payload)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	vol, err := volMgr.Get("test-vol")
	require.NoError(t, err)
	beforeBlocks := vol.AllocatedBlocks

	// Truncate to 0 bytes
	f2, err := fs.OpenFile("shrink.txt", os.O_RDWR|os.O_TRUNC, 0644)
	require.NoError(t, err)
	require.NoError(t, f2.Close())

	vol, err = volMgr.Get("test-vol")
	require.NoError(t, err)
	assert.Less(t, vol.AllocatedBlocks, beforeBlocks, "AllocatedBlocks should decrease after truncate")
}
