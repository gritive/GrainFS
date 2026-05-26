package volume

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

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

type blockingPartialBackend struct {
	storage.Backend
	storage.PartialIO

	blockKey string
	enabled  atomic.Bool
	entered  chan struct{}
	release  chan struct{}

	beforeReadAt atomic.Bool
}

func newBlockingPartialBackend(inner *storage.LocalBackend, blockKey string) *blockingPartialBackend {
	return &blockingPartialBackend{
		Backend:   inner,
		PartialIO: inner,
		blockKey:  blockKey,
		entered:   make(chan struct{}),
		release:   make(chan struct{}),
	}
}

func (b *blockingPartialBackend) ReadAt(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, error) {
	if b.enabled.Load() && bucket == volumeBucketName && key == b.blockKey {
		if b.beforeReadAt.Load() {
			close(b.entered)
			select {
			case <-b.release:
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		n, err := b.PartialIO.ReadAt(ctx, bucket, key, offset, buf)
		if !b.beforeReadAt.Load() {
			close(b.entered)
			select {
			case <-b.release:
			case <-ctx.Done():
				return n, ctx.Err()
			}
		}
		return n, err
	}
	return b.PartialIO.ReadAt(ctx, bucket, key, offset, buf)
}

func (b *blockingPartialBackend) PreferReadAt(bucket string) bool {
	return bucket == volumeBucketName
}

func TestReadAtSerializesWithConcurrentWriteAt(t *testing.T) {
	dir := t.TempDir()
	local, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { local.Close() })

	const name = "read-write-lock"
	backend := newBlockingPartialBackend(local, blockKey(name, 0))
	mgr := NewManager(backend)
	_, err = mgr.Create(name, int64(DefaultBlockSize))
	require.NoError(t, err)
	_, err = mgr.WriteAt(name, []byte("old-value"), 0)
	require.NoError(t, err)

	backend.enabled.Store(true)
	backend.beforeReadAt.Store(true)
	got := make([]byte, len("old-value"))
	readDone := make(chan error, 1)
	go func() {
		_, err := mgr.ReadAt(name, got, 0)
		readDone <- err
	}()

	select {
	case <-backend.entered:
	case <-time.After(time.Second):
		t.Fatal("ReadAt did not reach backend I/O")
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := mgr.WriteAt(name, []byte("new-value"), 0)
		writeDone <- err
	}()

	select {
	case err := <-writeDone:
		require.NoError(t, err)
		t.Fatal("WriteAt completed while ReadAt backend I/O was still blocked")
	case <-time.After(100 * time.Millisecond):
	}

	close(backend.release)
	require.NoError(t, <-readDone)
	require.Equal(t, "old-value", string(got))
	require.NoError(t, <-writeDone)
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

func TestDiscard(t *testing.T) {
	const blockSize = DefaultBlockSize
	mgr := setupManager(t)
	_, err := mgr.Create("discard-test", int64(blockSize*4))
	require.NoError(t, err)

	// 블록 0, 1, 2에 쓰기
	_, err = mgr.WriteAt("discard-test", make([]byte, blockSize*3), 0)
	require.NoError(t, err)

	vol, _ := mgr.Get("discard-test")
	require.Equal(t, int64(3), vol.AllocatedBlocks, "should have 3 blocks after write")

	// 블록 1만 discard (off=4096, length=4096)
	err = mgr.Discard("discard-test", int64(blockSize), int64(blockSize))
	require.NoError(t, err)

	vol, _ = mgr.Get("discard-test")
	assert.Equal(t, int64(2), vol.AllocatedBlocks, "counter should decrease by 1")

	// 블록 1 읽기는 zeros
	buf := make([]byte, blockSize)
	_, err = mgr.ReadAt("discard-test", buf, int64(blockSize))
	require.NoError(t, err)
	assert.Equal(t, make([]byte, blockSize), buf, "discarded block should read as zeros")
}

func TestDiscardPartialBlock(t *testing.T) {
	const blockSize = DefaultBlockSize
	mgr := setupManager(t)
	_, err := mgr.Create("partial-discard", int64(blockSize*4))
	require.NoError(t, err)

	_, err = mgr.WriteAt("partial-discard", make([]byte, blockSize*2), 0)
	require.NoError(t, err)

	// 부분 커버 discard — off=1, length=blockSize-1 → firstBlock=ceil(1/bs)=1, lastBlock=floor(bs/bs)-1=0 → 범위 없음
	err = mgr.Discard("partial-discard", 1, int64(blockSize-1))
	require.NoError(t, err)

	vol, _ := mgr.Get("partial-discard")
	assert.Equal(t, int64(2), vol.AllocatedBlocks, "partial discard should not free any block")
}

func TestDiscardNonExistentBlock(t *testing.T) {
	const blockSize = DefaultBlockSize
	mgr := setupManager(t)
	_, err := mgr.Create("no-block-discard", int64(blockSize*4))
	require.NoError(t, err)

	// 블록 쓰기 없이 discard → 에러 없이 통과
	err = mgr.Discard("no-block-discard", 0, int64(blockSize))
	require.NoError(t, err, "discard of non-existent block should be idempotent")
}

func TestDiscardCounterClamp(t *testing.T) {
	const blockSize = DefaultBlockSize
	mgr := setupManager(t)
	_, err := mgr.Create("clamp-test", int64(blockSize*4))
	require.NoError(t, err)

	// 블록 1개 쓰기 후 2블록 범위 discard
	_, err = mgr.WriteAt("clamp-test", make([]byte, blockSize), 0)
	require.NoError(t, err)

	vol, _ := mgr.Get("clamp-test")
	require.Equal(t, int64(1), vol.AllocatedBlocks)

	// 블록 0, 1 discard (블록 1은 미존재)
	err = mgr.Discard("clamp-test", 0, int64(blockSize*2))
	require.NoError(t, err)

	vol, _ = mgr.Get("clamp-test")
	assert.Equal(t, int64(0), vol.AllocatedBlocks, "counter should not go below 0")
}

func setupManagerWithQuota(t *testing.T, quota int64) *Manager {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	return NewManagerWithOptions(backend, ManagerOptions{PoolQuota: quota})
}

func TestPoolQuotaExceeded(t *testing.T) {
	const blockSize = DefaultBlockSize
	// quota = 2 blocks
	mgr := setupManagerWithQuota(t, int64(blockSize*2))
	_, err := mgr.Create("quota-test", int64(blockSize*4))
	require.NoError(t, err)

	// 2블록 쓰기 → 성공
	_, err = mgr.WriteAt("quota-test", make([]byte, blockSize*2), 0)
	require.NoError(t, err)

	// 1블록 더 쓰기 → quota 초과
	_, err = mgr.WriteAt("quota-test", make([]byte, blockSize), int64(blockSize*2))
	assert.ErrorIs(t, err, ErrPoolQuotaExceeded)
}

func TestPoolQuotaBoundary(t *testing.T) {
	const blockSize = DefaultBlockSize
	// quota = 2 blocks 정확히
	mgr := setupManagerWithQuota(t, int64(blockSize*2))
	_, err := mgr.Create("boundary-test", int64(blockSize*4))
	require.NoError(t, err)

	// 한 번에 2블록 쓰기 → 한도에 정확히 맞음 (통과)
	_, err = mgr.WriteAt("boundary-test", make([]byte, blockSize*2), 0)
	require.NoError(t, err, "write at exact quota limit should succeed")

	// 1바이트라도 새 블록 → 거부
	_, err = mgr.WriteAt("boundary-test", []byte{0x01}, int64(blockSize*2))
	assert.ErrorIs(t, err, ErrPoolQuotaExceeded, "write beyond quota limit should fail")
}

func TestPoolQuotaOverwriteDoesNotCount(t *testing.T) {
	const blockSize = DefaultBlockSize
	// quota = 1 block
	mgr := setupManagerWithQuota(t, int64(blockSize))
	_, err := mgr.Create("overwrite-quota", int64(blockSize*4))
	require.NoError(t, err)

	// 첫 쓰기 → 1블록 할당 (quota 소진)
	_, err = mgr.WriteAt("overwrite-quota", make([]byte, blockSize), 0)
	require.NoError(t, err)

	// 같은 블록 덮어쓰기 → 새 블록 없으므로 quota 검사 통과
	_, err = mgr.WriteAt("overwrite-quota", make([]byte, blockSize), 0)
	require.NoError(t, err, "overwrite of existing block should not be quota-limited")
}

// listErrBackend wraps LocalBackend but fails ListObjects and WalkObjects for block prefixes.
type listErrBackend struct {
	storage.Backend
}

func (b *listErrBackend) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	if prefix != metaPrefix {
		return nil, fmt.Errorf("injected list error")
	}
	return b.Backend.ListObjects(ctx, bucket, prefix, maxKeys)
}

func (b *listErrBackend) WalkObjects(ctx context.Context, bucket, prefix string, fn func(*storage.Object) error) error {
	if prefix != metaPrefix {
		return fmt.Errorf("injected list error")
	}
	return b.Backend.WalkObjects(ctx, bucket, prefix, fn)
}

func TestRecalculate(t *testing.T) {
	const blockSize = DefaultBlockSize

	t.Run("drift fixed", func(t *testing.T) {
		dir := t.TempDir()
		backend, err := storage.NewLocalBackend(dir)
		require.NoError(t, err)
		mgr := NewManager(backend)

		_, err = mgr.Create("drift-vol", int64(blockSize*10))
		require.NoError(t, err)

		// Write 3 blocks
		_, err = mgr.WriteAt("drift-vol", make([]byte, blockSize*3), 0)
		require.NoError(t, err)

		// Artificially drift AllocatedBlocks
		mgr.mu.Lock()
		mgr.volumes["drift-vol"].AllocatedBlocks = 99
		mgr.mu.Unlock()

		before, after, err := mgr.Recalculate("drift-vol")
		require.NoError(t, err)
		assert.Equal(t, int64(99), before)
		assert.Equal(t, int64(3), after)
	})

	t.Run("not found", func(t *testing.T) {
		mgr := setupManager(t)
		_, _, err := mgr.Recalculate("nonexistent")
		assert.Error(t, err)
	})

	t.Run("list error propagated", func(t *testing.T) {
		dir := t.TempDir()
		base, err := storage.NewLocalBackend(dir)
		require.NoError(t, err)
		mgr := NewManager(&listErrBackend{Backend: base})

		_, err = mgr.Create("err-vol", int64(blockSize*4))
		require.NoError(t, err)

		_, _, err = mgr.Recalculate("err-vol")
		assert.Error(t, err)
	})

	t.Run("no drift no write", func(t *testing.T) {
		dir := t.TempDir()
		backend, err := storage.NewLocalBackend(dir)
		require.NoError(t, err)
		mgr := NewManager(backend)

		_, err = mgr.Create("nodrift-vol", int64(blockSize*10))
		require.NoError(t, err)

		_, err = mgr.WriteAt("nodrift-vol", make([]byte, blockSize*3), 0)
		require.NoError(t, err)

		// AllocatedBlocks should already be 3 after WriteAt
		before, after, err := mgr.Recalculate("nodrift-vol")
		require.NoError(t, err)
		assert.Equal(t, before, after, "no drift: before and after must match")
	})
}

func TestNameFromBlockKey(t *testing.T) {
	cases := []struct {
		name   string
		key    string
		want   string
		wantOK bool
	}{
		{"valid block key", BlockKeyPrefix("v1") + "000000000001", "v1", true},
		{"valid block key big number", BlockKeyPrefix("vol-2") + "000000999999", "vol-2", true},
		{"empty key", "", "", false},
		{"missing meta prefix", "v1/blk_000000000001", "", false},
		{"meta prefix only", MetaPrefix, "", false},
		{"name without blk separator", MetaPrefix + "v1/meta", "", false},
		{"name without blk separator, no slash", MetaPrefix + "v1", "", false},
		{"empty name segment", MetaPrefix + "/blk_000000000001", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := NameFromBlockKey(tc.key)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}
