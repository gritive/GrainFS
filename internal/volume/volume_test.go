package volume

import (
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupManager(t *testing.T) *Manager {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	return NewManager(backend)
}

func TestCreateVolume(t *testing.T) {
	tests := []struct {
		name    string
		volName string
		size    int64
		wantErr bool
	}{
		{"basic create", "test-vol", 1024 * 1024, false},
		{"small volume", "small", 4096, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr := setupManager(t)
			vol, err := mgr.Create(tt.volName, tt.size)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.volName, vol.Name)
			assert.Equal(t, tt.size, vol.Size)
			assert.Equal(t, DefaultBlockSize, vol.BlockSize)
		})
	}
}

func TestCreateDuplicateVolume(t *testing.T) {
	mgr := setupManager(t)

	_, err := mgr.Create("dup", 4096)
	require.NoError(t, err)

	_, err = mgr.Create("dup", 4096)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestGetVolume(t *testing.T) {
	mgr := setupManager(t)

	_, err := mgr.Create("get-test", 8192)
	require.NoError(t, err)

	vol, err := mgr.Get("get-test")
	require.NoError(t, err)
	assert.Equal(t, "get-test", vol.Name)
	assert.Equal(t, int64(8192), vol.Size)
}

func TestGetNonexistentVolume(t *testing.T) {
	mgr := setupManager(t)
	_, err := mgr.Get("nonexistent")
	assert.Error(t, err)
}

func TestDeleteVolume(t *testing.T) {
	mgr := setupManager(t)

	_, err := mgr.Create("del-test", 4096)
	require.NoError(t, err)

	// Write some data first
	data := []byte("hello world")
	_, err = mgr.WriteAt("del-test", data, 0)
	require.NoError(t, err)

	err = mgr.Delete("del-test")
	require.NoError(t, err)

	// Volume should be gone
	_, err = mgr.Get("del-test")
	assert.Error(t, err)
}

func TestDeleteNonexistentVolume(t *testing.T) {
	mgr := setupManager(t)
	err := mgr.Delete("nonexistent")
	assert.Error(t, err)
}

func TestListVolumes(t *testing.T) {
	mgr := setupManager(t)

	// Empty list
	vols, err := mgr.List()
	require.NoError(t, err)
	assert.Empty(t, vols)

	// Create volumes
	_, err = mgr.Create("vol-a", 4096)
	require.NoError(t, err)
	_, err = mgr.Create("vol-b", 8192)
	require.NoError(t, err)

	vols, err = mgr.List()
	require.NoError(t, err)
	assert.Len(t, vols, 2)

	names := make(map[string]bool)
	for _, v := range vols {
		names[v.Name] = true
	}
	assert.True(t, names["vol-a"])
	assert.True(t, names["vol-b"])
}

func TestWriteAndReadAt(t *testing.T) {
	mgr := setupManager(t)

	_, err := mgr.Create("rw-test", 16384) // 4 blocks
	require.NoError(t, err)

	// Write data
	data := []byte("Hello, GrainFS Volume!")
	n, err := mgr.WriteAt("rw-test", data, 0)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)

	// Read it back
	buf := make([]byte, len(data))
	n, err = mgr.ReadAt("rw-test", buf, 0)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, buf)
}

func TestReadUnwrittenBlockReturnsZeros(t *testing.T) {
	mgr := setupManager(t)

	_, err := mgr.Create("zero-test", 8192)
	require.NoError(t, err)

	buf := make([]byte, 100)
	n, err := mgr.ReadAt("zero-test", buf, 0)
	require.NoError(t, err)
	assert.Equal(t, 100, n)

	for i, b := range buf {
		assert.Equal(t, byte(0), b, "byte at %d should be zero", i)
	}
}

func TestWriteAcrossBlockBoundary(t *testing.T) {
	mgr := setupManager(t)

	_, err := mgr.Create("cross-test", 16384)
	require.NoError(t, err)

	// Write data that spans block boundary (block size = 4096)
	offset := int64(DefaultBlockSize - 5)
	data := []byte("0123456789") // 10 bytes, 5 in block 0, 5 in block 1

	n, err := mgr.WriteAt("cross-test", data, offset)
	require.NoError(t, err)
	assert.Equal(t, 10, n)

	// Read it back
	buf := make([]byte, 10)
	n, err = mgr.ReadAt("cross-test", buf, offset)
	require.NoError(t, err)
	assert.Equal(t, 10, n)
	assert.Equal(t, data, buf)
}

func TestWriteBeyondVolumeSize(t *testing.T) {
	mgr := setupManager(t)

	_, err := mgr.Create("bound-test", 100)
	require.NoError(t, err)

	// Write that would extend beyond volume
	data := make([]byte, 50)
	for i := range data {
		data[i] = byte(i)
	}

	// Write starting at offset 80 - only 20 bytes should be written
	n, err := mgr.WriteAt("bound-test", data, 80)
	require.NoError(t, err)
	assert.Equal(t, 20, n)

	// Read back and verify
	buf := make([]byte, 20)
	n, err = mgr.ReadAt("bound-test", buf, 80)
	require.NoError(t, err)
	assert.Equal(t, 20, n)
	assert.Equal(t, data[:20], buf)
}

func TestReadAtEOF(t *testing.T) {
	mgr := setupManager(t)

	_, err := mgr.Create("eof-test", 100)
	require.NoError(t, err)

	buf := make([]byte, 10)
	_, err = mgr.ReadAt("eof-test", buf, 100) // exactly at end
	assert.Error(t, err)                      // should be io.EOF
}

func TestVolumeAllocatedBytes(t *testing.T) {
	tests := []struct {
		allocatedBlocks int64
		blockSize       int
		want            int64
	}{
		{-1, 4096, -1},   // untracked
		{0, 4096, 0},     // empty
		{3, 4096, 12288}, // 3 blocks * 4096
	}
	for _, tt := range tests {
		vol := &Volume{AllocatedBlocks: tt.allocatedBlocks, BlockSize: tt.blockSize}
		assert.Equal(t, tt.want, vol.AllocatedBytes())
	}
}

func TestAllocatedBlocksTracking(t *testing.T) {
	mgr := setupManager(t)
	_, err := mgr.Create("track-test", 16384) // 4 blocks of 4096
	require.NoError(t, err)

	// 새 볼륨은 untracked (-1)
	vol, err := mgr.Get("track-test")
	require.NoError(t, err)
	assert.Equal(t, int64(-1), vol.AllocatedBlocks)

	// 블록 0에 쓰기 → AllocatedBlocks = 1
	_, err = mgr.WriteAt("track-test", []byte("hello"), 0)
	require.NoError(t, err)

	vol, err = mgr.Get("track-test")
	require.NoError(t, err)
	assert.Equal(t, int64(1), vol.AllocatedBlocks)
	assert.Equal(t, int64(4096), vol.AllocatedBytes())

	// 동일 블록 덮어쓰기 → AllocatedBlocks 변화 없음
	_, err = mgr.WriteAt("track-test", []byte("world"), 0)
	require.NoError(t, err)

	vol, err = mgr.Get("track-test")
	require.NoError(t, err)
	assert.Equal(t, int64(1), vol.AllocatedBlocks, "overwrite should not increment counter")

	// 블록 1에 쓰기 → AllocatedBlocks = 2
	_, err = mgr.WriteAt("track-test", []byte("new block"), int64(DefaultBlockSize))
	require.NoError(t, err)

	vol, err = mgr.Get("track-test")
	require.NoError(t, err)
	assert.Equal(t, int64(2), vol.AllocatedBlocks)
}
