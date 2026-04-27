package volume

import (
	"bytes"
	"fmt"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume/dedup"
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

func (b *listErrBackend) ListObjects(bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	if prefix != metaPrefix {
		return nil, fmt.Errorf("injected list error")
	}
	return b.Backend.ListObjects(bucket, prefix, maxKeys)
}

func (b *listErrBackend) WalkObjects(bucket, prefix string, fn func(*storage.Object) error) error {
	if prefix != metaPrefix {
		return fmt.Errorf("injected list error")
	}
	return b.Backend.WalkObjects(bucket, prefix, fn)
}

// --- CoW Snapshot tests ---

func TestCreateSnapshot(t *testing.T) {
	const blockSize = DefaultBlockSize
	mgr := setupManager(t)
	_, err := mgr.Create("snap-vol", int64(blockSize*4))
	require.NoError(t, err)

	// 블록 0, 1 쓰기
	_, err = mgr.WriteAt("snap-vol", bytes.Repeat([]byte{0xAA}, blockSize*2), 0)
	require.NoError(t, err)

	snapID, err := mgr.CreateSnapshot("snap-vol")
	require.NoError(t, err)
	assert.NotEmpty(t, snapID)

	vol, err := mgr.Get("snap-vol")
	require.NoError(t, err)
	assert.Equal(t, int32(1), vol.SnapshotCount)
}

func TestCreateSnapshotNonexistent(t *testing.T) {
	mgr := setupManager(t)
	_, err := mgr.CreateSnapshot("no-such-vol")
	assert.Error(t, err)
}

func TestListSnapshots(t *testing.T) {
	const blockSize = DefaultBlockSize
	mgr := setupManager(t)
	_, err := mgr.Create("list-snap-vol", int64(blockSize*4))
	require.NoError(t, err)

	// 스냅샷 없을 때
	snaps, err := mgr.ListSnapshots("list-snap-vol")
	require.NoError(t, err)
	assert.Empty(t, snaps)

	// 스냅샷 2개 생성
	id1, err := mgr.CreateSnapshot("list-snap-vol")
	require.NoError(t, err)
	id2, err := mgr.CreateSnapshot("list-snap-vol")
	require.NoError(t, err)

	snaps, err = mgr.ListSnapshots("list-snap-vol")
	require.NoError(t, err)
	assert.Len(t, snaps, 2)

	ids := map[string]bool{}
	for _, s := range snaps {
		ids[s.ID] = true
	}
	assert.True(t, ids[id1])
	assert.True(t, ids[id2])
}

func TestSnapshotIsolation(t *testing.T) {
	const blockSize = DefaultBlockSize
	mgr := setupManager(t)
	_, err := mgr.Create("iso-vol", int64(blockSize*4))
	require.NoError(t, err)

	// 초기 데이터 쓰기
	original := bytes.Repeat([]byte{0xAA}, blockSize)
	_, err = mgr.WriteAt("iso-vol", original, 0)
	require.NoError(t, err)

	// 스냅샷 생성
	snapID, err := mgr.CreateSnapshot("iso-vol")
	require.NoError(t, err)

	// 스냅샷 후 블록 0 덮어쓰기
	modified := bytes.Repeat([]byte{0xBB}, blockSize)
	_, err = mgr.WriteAt("iso-vol", modified, 0)
	require.NoError(t, err)

	// 현재 상태 확인: 0xBB
	buf := make([]byte, blockSize)
	_, err = mgr.ReadAt("iso-vol", buf, 0)
	require.NoError(t, err)
	assert.Equal(t, modified, buf, "current state should be modified (0xBB)")

	// Rollback 후 블록 0은 0xAA 복원
	err = mgr.Rollback("iso-vol", snapID)
	require.NoError(t, err)

	_, err = mgr.ReadAt("iso-vol", buf, 0)
	require.NoError(t, err)
	assert.Equal(t, original, buf, "after rollback should be original (0xAA)")
}

func TestDeleteSnapshot(t *testing.T) {
	const blockSize = DefaultBlockSize
	mgr := setupManager(t)
	_, err := mgr.Create("del-snap-vol", int64(blockSize*4))
	require.NoError(t, err)

	snapID, err := mgr.CreateSnapshot("del-snap-vol")
	require.NoError(t, err)

	err = mgr.DeleteSnapshot("del-snap-vol", snapID)
	require.NoError(t, err)

	vol, err := mgr.Get("del-snap-vol")
	require.NoError(t, err)
	assert.Equal(t, int32(0), vol.SnapshotCount)

	snaps, err := mgr.ListSnapshots("del-snap-vol")
	require.NoError(t, err)
	assert.Empty(t, snaps)
}

func TestDeleteSnapshotNonexistent(t *testing.T) {
	mgr := setupManager(t)
	_, err := mgr.Create("ds-vol", 4096)
	require.NoError(t, err)

	err = mgr.DeleteSnapshot("ds-vol", "nonexistent-id")
	assert.Error(t, err)
}

func TestClone(t *testing.T) {
	const blockSize = DefaultBlockSize
	mgr := setupManager(t)
	_, err := mgr.Create("src-clone", int64(blockSize*4))
	require.NoError(t, err)

	// src에 데이터 쓰기
	data := bytes.Repeat([]byte{0xCC}, blockSize)
	_, err = mgr.WriteAt("src-clone", data, 0)
	require.NoError(t, err)

	// 클론 생성
	err = mgr.Clone("src-clone", "dst-clone")
	require.NoError(t, err)

	// dst에서 데이터 읽기 확인
	buf := make([]byte, blockSize)
	_, err = mgr.ReadAt("dst-clone", buf, 0)
	require.NoError(t, err)
	assert.Equal(t, data, buf)

	// dst 수정이 src에 영향 없는지 확인
	_, err = mgr.WriteAt("dst-clone", bytes.Repeat([]byte{0xDD}, blockSize), 0)
	require.NoError(t, err)

	_, err = mgr.ReadAt("src-clone", buf, 0)
	require.NoError(t, err)
	assert.Equal(t, data, buf, "src should be unaffected by dst write")
}

func TestCloneNonexistent(t *testing.T) {
	mgr := setupManager(t)
	err := mgr.Clone("no-such-src", "dst")
	assert.Error(t, err)
}

func TestNoOverheadWithoutSnapshots(t *testing.T) {
	// snapshot_count == 0인 볼륨은 live_map 없이 기존 블록키 사용 확인
	const blockSize = DefaultBlockSize
	mgr := setupManager(t)
	_, err := mgr.Create("no-snap-vol", int64(blockSize*4))
	require.NoError(t, err)

	data := bytes.Repeat([]byte{0xEE}, blockSize)
	_, err = mgr.WriteAt("no-snap-vol", data, 0)
	require.NoError(t, err)

	// liveMaps에 nil 엔트리가 없거나 nil이어야 함 (live_map 비활성)
	mgr.mu.Lock()
	lm := mgr.liveMaps["no-snap-vol"]
	mgr.mu.Unlock()
	assert.Nil(t, lm, "volume with no snapshots should have nil live_map cache")

	buf := make([]byte, blockSize)
	_, err = mgr.ReadAt("no-snap-vol", buf, 0)
	require.NoError(t, err)
	assert.Equal(t, data, buf)
}

func TestLiveMapParseRoundTrip(t *testing.T) {
	lm := map[int64]string{
		0: "__vol/vol1/blk_000000000000",
		5: "__vol/vol1/blk_000000000005_v018f4e7c-1d9e-7bf6-b5d1-2e3c8f9a0b1c",
	}

	var buf bytes.Buffer
	err := serializeLiveMap(lm, &buf)
	require.NoError(t, err)

	parsed, err := parseLiveMap(&buf)
	require.NoError(t, err)
	assert.Equal(t, lm, parsed)
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

// --- Dedup integration tests ---

func openTestBadger(t *testing.T) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func setupDedupManager(t *testing.T) *Manager {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	idx := dedup.NewBadgerIndex(openTestBadger(t))
	return NewManagerWithOptions(backend, ManagerOptions{DedupIndex: idx})
}

// countObjects returns the number of S3 objects under the given prefix.
func countObjects(t *testing.T, m *Manager, prefix string) int {
	t.Helper()
	var count int
	err := m.backend.WalkObjects(volumeBucketName, prefix, func(_ *storage.Object) error {
		count++
		return nil
	})
	require.NoError(t, err)
	return count
}

// TestDedupWriteSameContent writes the same 4KB block twice: the second write
// must reuse the existing S3 object (refcount→2, no extra PutObject).
func TestDedupWriteSameContent(t *testing.T) {
	mgr := setupDedupManager(t)
	const volName = "dedup-vol"
	_, err := mgr.Create(volName, int64(DefaultBlockSize*4))
	require.NoError(t, err)

	data := bytes.Repeat([]byte{0xAB}, DefaultBlockSize)

	// First write: block 0
	_, err = mgr.WriteAt(volName, data, 0)
	require.NoError(t, err)

	// Second write: same content to block 1
	_, err = mgr.WriteAt(volName, data, int64(DefaultBlockSize))
	require.NoError(t, err)

	// Both logical blocks should read back correctly
	buf := make([]byte, DefaultBlockSize)
	_, err = mgr.ReadAt(volName, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, data, buf)

	_, err = mgr.ReadAt(volName, buf, int64(DefaultBlockSize))
	require.NoError(t, err)
	assert.Equal(t, data, buf)

	// Only one S3 block object should exist (dedup hit)
	blockObjs := countObjects(t, mgr, blockPrefix(volName))
	assert.Equal(t, 1, blockObjs, "dedup: same content block must share one S3 object")
}

// TestDedupDiscardReleasesRef verifies that Discard decrements the refcount,
// and only deletes the S3 object when the last reference is removed.
func TestDedupDiscardReleasesRef(t *testing.T) {
	mgr := setupDedupManager(t)
	const volName = "dedup-discard"
	_, err := mgr.Create(volName, int64(DefaultBlockSize*4))
	require.NoError(t, err)

	data := bytes.Repeat([]byte{0xCD}, DefaultBlockSize)

	// Write same content to blocks 0 and 1 → refcount = 2, one S3 object
	_, err = mgr.WriteAt(volName, data, 0)
	require.NoError(t, err)
	_, err = mgr.WriteAt(volName, data, int64(DefaultBlockSize))
	require.NoError(t, err)

	before := countObjects(t, mgr, blockPrefix(volName))
	require.Equal(t, 1, before)

	// Discard block 0 → refcount → 1, S3 object must survive
	err = mgr.Discard(volName, 0, int64(DefaultBlockSize))
	require.NoError(t, err)
	assert.Equal(t, 1, countObjects(t, mgr, blockPrefix(volName)), "S3 object must survive while ref remains")

	// Discard block 1 → refcount → 0, S3 object must be deleted
	err = mgr.Discard(volName, int64(DefaultBlockSize), int64(DefaultBlockSize))
	require.NoError(t, err)
	assert.Equal(t, 0, countObjects(t, mgr, blockPrefix(volName)), "last ref gone: S3 object must be deleted")
}

// TestDedupOverwriteReleasesOldRef writes block 0 twice with different content.
// The overwrite must release the old object's refcount (→0 → deleted) and
// register the new content as a fresh S3 object.
func TestDedupOverwriteReleasesOldRef(t *testing.T) {
	mgr := setupDedupManager(t)
	const volName = "dedup-overwrite"
	_, err := mgr.Create(volName, int64(DefaultBlockSize*4))
	require.NoError(t, err)

	data1 := bytes.Repeat([]byte{0x11}, DefaultBlockSize)
	data2 := bytes.Repeat([]byte{0x22}, DefaultBlockSize)

	// First write
	_, err = mgr.WriteAt(volName, data1, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, countObjects(t, mgr, blockPrefix(volName)))

	// Overwrite with different content: old object refcount→0 → deleted; new object created
	_, err = mgr.WriteAt(volName, data2, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, countObjects(t, mgr, blockPrefix(volName)), "one new object after overwrite")

	// Read back must return data2
	buf := make([]byte, DefaultBlockSize)
	_, err = mgr.ReadAt(volName, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, data2, buf)
}

// TestAllocatedBlocks_Dedup_Counts_Unique_Objects verifies that AllocatedBlocks
// reflects unique S3 objects, not block positions. Writing the same content to
// 4 positions must leave AllocatedBlocks == 1.
func TestAllocatedBlocks_Dedup_Counts_Unique_Objects(t *testing.T) {
	mgr := setupDedupManager(t)
	const volName = "alloc-dedup-unique"
	_, err := mgr.Create(volName, int64(DefaultBlockSize*8))
	require.NoError(t, err)

	data := bytes.Repeat([]byte{0xAB}, DefaultBlockSize)

	for i := 0; i < 4; i++ {
		_, err = mgr.WriteAt(volName, data, int64(i)*int64(DefaultBlockSize))
		require.NoError(t, err)
	}

	vol, err := mgr.Get(volName)
	require.NoError(t, err)
	assert.Equal(t, int64(1), vol.AllocatedBlocks,
		"4 writes of identical content must allocate exactly 1 S3 object")
}

// TestAllocatedBlocks_Dedup_Discard_OnlyOnObjectDelete verifies that Discard
// only decrements AllocatedBlocks when the last reference to an S3 object is removed.
func TestAllocatedBlocks_Dedup_Discard_OnlyOnObjectDelete(t *testing.T) {
	mgr := setupDedupManager(t)
	const volName = "alloc-dedup-discard"
	_, err := mgr.Create(volName, int64(DefaultBlockSize*4))
	require.NoError(t, err)

	data := bytes.Repeat([]byte{0xCD}, DefaultBlockSize)

	_, err = mgr.WriteAt(volName, data, 0)
	require.NoError(t, err)
	_, err = mgr.WriteAt(volName, data, int64(DefaultBlockSize))
	require.NoError(t, err)

	vol, err := mgr.Get(volName)
	require.NoError(t, err)
	require.Equal(t, int64(1), vol.AllocatedBlocks, "setup: 2 positions, 1 S3 object")

	// Discard block 0: refcount 2→1, S3 object survives → AllocatedBlocks must stay 1
	err = mgr.Discard(volName, 0, int64(DefaultBlockSize))
	require.NoError(t, err)
	vol, err = mgr.Get(volName)
	require.NoError(t, err)
	assert.Equal(t, int64(1), vol.AllocatedBlocks,
		"freeing one of two references must not decrement AllocatedBlocks")

	// Discard block 1: refcount 1→0, S3 object deleted → AllocatedBlocks must reach 0
	err = mgr.Discard(volName, int64(DefaultBlockSize), int64(DefaultBlockSize))
	require.NoError(t, err)
	vol, err = mgr.Get(volName)
	require.NoError(t, err)
	assert.Equal(t, int64(0), vol.AllocatedBlocks,
		"freeing last reference must decrement AllocatedBlocks")
}

// TestAllocatedBlocks_Dedup_Overwrite_NetZero verifies that overwriting a block
// with unique new content keeps AllocatedBlocks stable (old S3 object freed, new one created).
func TestAllocatedBlocks_Dedup_Overwrite_NetZero(t *testing.T) {
	mgr := setupDedupManager(t)
	const volName = "alloc-dedup-overwrite"
	_, err := mgr.Create(volName, int64(DefaultBlockSize*4))
	require.NoError(t, err)

	dataA := bytes.Repeat([]byte{0x11}, DefaultBlockSize)
	dataB := bytes.Repeat([]byte{0x22}, DefaultBlockSize)

	_, err = mgr.WriteAt(volName, dataA, 0)
	require.NoError(t, err)
	vol, err := mgr.Get(volName)
	require.NoError(t, err)
	require.Equal(t, int64(1), vol.AllocatedBlocks, "initial write")

	// Overwrite with different unique content: A freed (refcount→0), B created.
	// Net change to S3 objects = 0 → AllocatedBlocks must stay 1.
	_, err = mgr.WriteAt(volName, dataB, 0)
	require.NoError(t, err)
	vol, err = mgr.Get(volName)
	require.NoError(t, err)
	assert.Equal(t, int64(1), vol.AllocatedBlocks,
		"overwrite unique content: one freed, one created, net 0")
}

// TestDedupPartialWritePreservesUntouchedBytes is the regression test for the
// P0 data-loss bug: a sub-block partial write in dedup mode must load the
// existing block via its canonical key before merging, not via physicalKey.
// Without the fix, the untouched tail of the block would be zeroed out.
func TestDedupPartialWritePreservesUntouchedBytes(t *testing.T) {
	mgr := setupDedupManager(t)
	const volName = "dedup-partial"
	_, err := mgr.Create(volName, int64(DefaultBlockSize*4))
	require.NoError(t, err)

	// Fill block 0 with a recognisable pattern.
	full := bytes.Repeat([]byte{0xAA}, DefaultBlockSize)
	_, err = mgr.WriteAt(volName, full, 0)
	require.NoError(t, err)

	// Partial overwrite: write 512 bytes of 0xBB at intra-block offset 512.
	// Only bytes [512, 1024) should change; the rest must stay 0xAA.
	const patchOffset = 512
	const patchLen = 512
	patch := bytes.Repeat([]byte{0xBB}, patchLen)
	_, err = mgr.WriteAt(volName, patch, patchOffset)
	require.NoError(t, err)

	// Read back the whole block and verify byte-by-byte.
	got := make([]byte, DefaultBlockSize)
	_, err = mgr.ReadAt(volName, got, 0)
	require.NoError(t, err)

	assert.Equal(t, full[:patchOffset], got[:patchOffset], "bytes before patch must be unchanged (0xAA)")
	assert.Equal(t, patch, got[patchOffset:patchOffset+patchLen], "patched region must be 0xBB")
	assert.Equal(t, full[patchOffset+patchLen:], got[patchOffset+patchLen:], "bytes after patch must be unchanged (0xAA)")
}
