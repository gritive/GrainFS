package volume

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gritive/GrainFS/internal/cache/blockcache"
	"github.com/gritive/GrainFS/internal/pool"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume/dedup"
)

// ErrNotFound is returned when a volume does not exist.
var ErrNotFound = errors.New("volume not found")

const (
	DefaultBlockSize = 4096
	volumeBucketName = "__grainfs_volumes"
	metaPrefix       = "__vol/"
)

// VolumeBucketName is the storage backend bucket where all volume metadata and
// blocks are stored. Exported for callers that need to construct backend keys
// (e.g. admin handlers correlating incidents to a volume).
const VolumeBucketName = volumeBucketName

// BlockKeyPrefix returns the key prefix under which a volume's data blocks are
// stored within VolumeBucketName.
func BlockKeyPrefix(name string) string { return blockPrefix(name) }

// NameFromBlockKey extracts the volume name from a block-storage key produced
// by blockPrefix/blockKey (format: "__vol/{name}/blk_{N}"). Returns "", false
// when key is not a block key.
func NameFromBlockKey(key string) (string, bool) {
	if len(key) <= len(metaPrefix) || key[:len(metaPrefix)] != metaPrefix {
		return "", false
	}
	rest := key[len(metaPrefix):]
	idx := strings.Index(rest, "/blk_")
	if idx <= 0 {
		return "", false
	}
	return rest[:idx], true
}

// MetaPrefix is the shared key prefix for every volume's metadata and blocks
// inside VolumeBucketName. Useful as the walk root for cross-volume operations
// (e.g. scrub).
const MetaPrefix = metaPrefix

// Volume represents a virtual block device backed by object storage.
type Volume struct {
	Name            string
	Size            int64
	BlockSize       int
	AllocatedBlocks int64 // -1=untracked, 0=empty, >0=block count
	SnapshotCount   int32 // 0=no snapshots; >0 means live_map is active
}

// AllocatedBytes returns the number of bytes allocated in backing storage.
// Returns -1 if the volume was created before allocation tracking was introduced.
func (v *Volume) AllocatedBytes() int64 {
	if v.AllocatedBlocks < 0 {
		return -1
	}
	return v.AllocatedBlocks * int64(v.BlockSize)
}

// ManagerOptions configures optional Manager behaviour.
type ManagerOptions struct {
	// PoolQuota is the maximum total allocated bytes across all volumes.
	// 0 means unlimited (default).
	PoolQuota int64

	// DedupIndex enables block-level deduplication. nil disables dedup.
	DedupIndex dedup.DedupIndex

	// BlockCache, if non-nil, sits in front of backend.GetObject on
	// the ReadAt path. Hits skip the storage backend entirely; misses
	// populate the cache so subsequent reads of the same physical
	// block are free. Pass `blockcache.New(0)` (or leave nil) to
	// disable. Sizing guidance is in the blockcache package doc.
	BlockCache *blockcache.Cache
}

// SnapshotInfo describes a point-in-time snapshot of a volume.
type SnapshotInfo struct {
	ID         string
	CreatedAt  time.Time
	BlockCount int64 // number of allocated blocks at snapshot creation time
}

// Manager manages volumes on top of a storage.Backend.
type Manager struct {
	backend   storage.Backend
	mu        sync.Mutex                  // 단일 뮤텍스: read-modify-write 원자성 보장. lock-free actor 검토 — Manager는 메타·dedup·live_map 다중 자료구조의 cross-volume 원자성을 요구하고, 모든 read/write/snapshot operations이 통과하므로 channel-based actor는 핫패스 round-trip 비용 + 메타 RMW 시퀀싱 복잡도가 mutex보다 큼. 추후 sharded mutex 분리 검토 가능.
	volumes   map[string]*Volume          // 인메모리 캐시
	liveMaps  map[string]map[int64]string // cache: absent=未読み込み, nil entry=live_map 없음
	dedup     dedup.DedupIndex            // nil = dedup 비활성화
	blocks    *blockcache.Cache           // nil = block cache 비활성. ReadAt이 backend 앞에 두는 LRU.
	opts      ManagerOptions
	blkPool   *pool.Pool[[]byte] // reusable DefaultBlockSize-byte slices for ReadAt/WriteAt
	snapStore SnapshotStore
}

// NewManager creates a new volume manager.
func NewManager(backend storage.Backend) *Manager {
	return NewManagerWithOptions(backend, ManagerOptions{})
}

// NewManagerWithOptions creates a new volume manager with the given options.
func NewManagerWithOptions(backend storage.Backend, opts ManagerOptions) *Manager {
	m := &Manager{
		backend:  backend,
		volumes:  make(map[string]*Volume),
		liveMaps: make(map[string]map[int64]string),
		dedup:    opts.DedupIndex,
		blocks:   opts.BlockCache,
		opts:     opts,
		blkPool:  pool.New(func() []byte { return make([]byte, DefaultBlockSize) }),
	}
	m.snapStore = newS3SnapshotStore(m)
	return m
}

// getBlkBuf returns a zeroed block buffer from the pool, or allocates one for non-default sizes.
func (m *Manager) getBlkBuf(blockSize int) []byte {
	if blockSize == DefaultBlockSize {
		b := m.blkPool.Get()
		clear(b)
		return b
	}
	return make([]byte, blockSize)
}

// putBlkBuf returns a block buffer to the pool (only for default-size buffers).
func (m *Manager) putBlkBuf(b []byte) {
	if len(b) == DefaultBlockSize {
		m.blkPool.Put(b)
	}
}

func (m *Manager) newBlockIOEngine() blockIOEngine {
	engine := blockIOEngine{
		objects:   backendBlockObjectStore{backend: m.backend},
		dedup:     m.dedup,
		meter:     defaultBlockReadMeter{},
		getBlkBuf: m.getBlkBuf,
		putBlkBuf: m.putBlkBuf,
	}
	if m.blocks != nil {
		engine.cache = m.blocks
	}
	return engine
}

func (m *Manager) currentAllocatedBytesUnlocked() int64 {
	var total int64
	for _, v := range m.volumes {
		if v.AllocatedBlocks > 0 {
			total += v.AllocatedBlocks * int64(v.BlockSize)
		}
	}
	return total
}

func (m *Manager) applyBlockIOResultUnlocked(name string, vol *Volume, result blockIOResult, liveMap map[int64]string, strictLiveMap, strictMetadata, trackUntracked bool) error {
	if result.LiveMapDirty {
		if err := m.persistLiveMapUnlocked(name, liveMap); err != nil {
			if strictLiveMap {
				return fmt.Errorf("persist live_map: %w", err)
			}
		}
	}

	if result.AllocationBytesDelta != 0 {
		if vol.AllocatedBlocks < 0 {
			if !trackUntracked {
				return nil
			}
			vol.AllocatedBlocks = 0
		}
		vol.AllocatedBlocks += result.AllocationBytesDelta / int64(vol.BlockSize)
		if vol.AllocatedBlocks < 0 {
			vol.AllocatedBlocks = 0
		}
		data, err := marshalVolume(vol)
		if err != nil {
			if strictMetadata {
				return err
			}
			return nil
		}
		if _, err := m.backend.PutObject(context.Background(), volumeBucketName, metaKey(name), bytes.NewReader(data), "application/protobuf"); err != nil {
			if strictMetadata {
				return fmt.Errorf("persist volume metadata: %w", err)
			}
		}
	}

	return nil
}

type readAtPreference interface {
	PreferReadAt(bucket string) bool
}

type writeAtPreference interface {
	PreferWriteAt(bucket string) bool
}

func backendPrefersReadAt(backend storage.Backend, bucket string) bool {
	pref, ok := backend.(readAtPreference)
	return ok && pref.PreferReadAt(bucket)
}

func backendPrefersWriteAt(backend storage.Backend, bucket string) bool {
	pref, ok := backend.(writeAtPreference)
	return ok && pref.PreferWriteAt(bucket)
}

// Create creates a new volume with the given name and size in bytes.
func (m *Manager) Create(name string, sizeBytes int64) (*Volume, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.ensureBucket(); err != nil {
		return nil, fmt.Errorf("ensure volume bucket: %w", err)
	}

	// Check if volume already exists
	if _, _, err := m.backend.GetObject(context.Background(), volumeBucketName, metaKey(name)); err == nil {
		return nil, fmt.Errorf("volume %q already exists", name)
	}

	vol := &Volume{
		Name:            name,
		Size:            sizeBytes,
		BlockSize:       DefaultBlockSize,
		AllocatedBlocks: -1, // untracked until first write
	}

	data, err := marshalVolume(vol)
	if err != nil {
		return nil, fmt.Errorf("marshal volume metadata: %w", err)
	}

	if _, err := m.backend.PutObject(context.Background(), volumeBucketName, metaKey(name), bytes.NewReader(data), "application/protobuf"); err != nil {
		return nil, fmt.Errorf("store volume metadata: %w", err)
	}

	m.volumes[name] = vol
	cp := *vol
	return &cp, nil
}

// Get returns volume metadata.
func (m *Manager) Get(name string) (*Volume, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	vol, err := m.getVolUnlocked(name)
	if err != nil {
		return nil, err
	}
	cp := *vol
	return &cp, nil
}

// Resize changes the logical size of a volume. Grow only — shrink is rejected
// with ErrShrinkNotSupported. Sparse layout means backend blocks are unaffected
// and snapshots/live_map remain valid for any new size >= old size. On persist
// failure the in-memory size is rolled back so memory and disk stay consistent.
func (m *Manager) Resize(name string, newSize int64) error {
	if newSize <= 0 {
		return fmt.Errorf("size %d: %w", newSize, ErrInvalidSize)
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	vol, err := m.getVolUnlocked(name)
	if err != nil {
		return err
	}
	if newSize < vol.Size {
		return fmt.Errorf("current=%d new=%d: %w", vol.Size, newSize, ErrShrinkNotSupported)
	}
	if newSize == vol.Size {
		return nil // no-op
	}

	oldSize := vol.Size
	vol.Size = newSize

	data, err := marshalVolume(vol)
	if err != nil {
		vol.Size = oldSize
		return fmt.Errorf("marshal volume: %w", err)
	}
	if _, err := m.backend.PutObject(context.Background(), volumeBucketName, metaKey(name), bytes.NewReader(data), "application/protobuf"); err != nil {
		vol.Size = oldSize
		return fmt.Errorf("persist volume meta: %w", err)
	}
	return nil
}

// Delete deletes a volume and all its blocks.
func (m *Manager) Delete(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.deleteUnlocked(name)
}

// DeleteWithSnapshots deletes the volume and all its snapshots atomically under
// a single mutex acquisition. Used by `grainfs volume delete --force`. Use Delete
// when no snapshots are expected; this returns the same error as Delete if the
// volume has no snapshots.
func (m *Manager) DeleteWithSnapshots(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, err := m.getVolUnlocked(name); err != nil {
		return err
	}
	snaps, err := m.listSnapshotsUnlocked(name)
	if err != nil {
		return fmt.Errorf("list snapshots: %w", err)
	}
	for _, s := range snaps {
		if err := m.deleteSnapshotUnlocked(name, s.ID); err != nil {
			return fmt.Errorf("delete snapshot %s: %w", s.ID, err)
		}
	}
	return m.deleteUnlocked(name)
}

func (m *Manager) deleteUnlocked(name string) error {
	// Verify volume exists
	if _, _, err := m.backend.GetObject(context.Background(), volumeBucketName, metaKey(name)); err != nil {
		return fmt.Errorf("volume %q: %w", name, ErrNotFound)
	}

	// Delete all block objects, respecting dedup refcounts.
	if m.dedup != nil {
		toDelete, err := m.dedup.DeleteVolume(name)
		if err != nil {
			return fmt.Errorf("dedup delete volume: %w", err)
		}
		for _, key := range toDelete {
			m.backend.DeleteObject(context.Background(), volumeBucketName, key) //nolint:errcheck
		}
	} else {
		_ = m.backend.WalkObjects(context.Background(), volumeBucketName, blockPrefix(name), func(obj *storage.Object) error {
			_ = m.backend.DeleteObject(context.Background(), volumeBucketName, obj.Key)
			return nil
		})
	}

	if err := m.backend.DeleteObject(context.Background(), volumeBucketName, metaKey(name)); err != nil {
		return err
	}
	delete(m.volumes, name)
	delete(m.liveMaps, name)
	return nil
}

// List returns all volumes.
func (m *Manager) List() ([]*Volume, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.backend.HeadBucket(context.Background(), volumeBucketName); err != nil {
		return nil, nil // no volumes bucket yet
	}

	objs, err := m.backend.ListObjects(context.Background(), volumeBucketName, metaPrefix, 10000)
	if err != nil {
		return nil, fmt.Errorf("list volume metadata: %w", err)
	}

	var volumes []*Volume
	for _, obj := range objs {
		if !strings.HasSuffix(obj.Key, "/meta") {
			continue
		}
		rc, _, err := m.backend.GetObject(context.Background(), volumeBucketName, obj.Key)
		if err != nil {
			continue
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			continue
		}
		vol, err := unmarshalVolume(data)
		if err != nil {
			continue
		}
		m.volumes[vol.Name] = vol
		cp := *vol
		volumes = append(volumes, &cp)
	}
	return volumes, nil
}

// ReadAt reads len(p) bytes from the volume starting at byte offset off.
func (m *Manager) ReadAt(name string, p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	vol, err := m.getVolUnlocked(name)
	if err != nil {
		return 0, err
	}

	if off < 0 || off >= vol.Size {
		return 0, io.EOF
	}

	liveMap, err := m.getLiveMapUnlocked(name)
	if err != nil {
		return 0, fmt.Errorf("load live_map: %w", err)
	}

	result, err := m.newBlockIOEngine().read(name, vol, p, off, liveMap)
	return result.Bytes, err
}

// WriteAt writes len(p) bytes to the volume starting at byte offset off.
func (m *Manager) WriteAt(name string, p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	vol, err := m.getVolUnlocked(name)
	if err != nil {
		return 0, err
	}

	if off < 0 || off >= vol.Size {
		return 0, fmt.Errorf("offset %d out of range [0, %d)", off, vol.Size)
	}

	var liveMap map[int64]string
	if m.dedup == nil {
		liveMap, err = m.getLiveMapUnlocked(name)
		if err != nil {
			return 0, fmt.Errorf("load live_map: %w", err)
		}
	}

	result, err := m.newBlockIOEngine().write(name, vol, p, off, liveMap, m.currentAllocatedBytesUnlocked(), m.opts.PoolQuota)
	if err != nil {
		return result.Bytes, err
	}
	if err := m.applyBlockIOResultUnlocked(name, vol, result, liveMap, true, false, true); err != nil {
		return result.Bytes, err
	}
	return result.Bytes, nil
}

// asyncPutter is an optional interface for write-back NBD.
// Backends that implement it split the write into a fast local path and a
// deferred Raft commit. NBD accumulates commitFns and drains them on flush.
type asyncPutter interface {
	PutObjectAsync(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, func() error, error)
}

// WriteAtDeferred is the write-back variant of WriteAt.
// It writes data to local storage immediately and returns Raft commit functions
// that the caller must invoke (concurrently) on NBD_CMD_FLUSH.
// Falls back to synchronous WriteAt when the backend doesn't implement asyncPutter
// or when dedup/CoW is active.
func (m *Manager) WriteAtDeferred(name string, p []byte, off int64) ([]func() error, int, error) {
	ap, ok := m.backend.(asyncPutter)
	if !ok {
		n, err := m.WriteAt(name, p, off)
		return nil, n, err
	}

	m.mu.Lock()

	vol, err := m.getVolUnlocked(name)
	if err != nil {
		m.mu.Unlock()
		return nil, 0, err
	}
	if off < 0 || off >= vol.Size {
		m.mu.Unlock()
		return nil, 0, fmt.Errorf("offset %d out of range [0, %d)", off, vol.Size)
	}

	// Async path only supports the non-dedup, non-CoW fallback.
	if m.dedup != nil || vol.SnapshotCount > 0 || m.opts.PoolQuota > 0 {
		m.mu.Unlock()
		n, err := m.WriteAt(name, p, off)
		return nil, n, err
	}

	liveMap, err := m.getLiveMapUnlocked(name)
	if err != nil {
		m.mu.Unlock()
		return nil, 0, fmt.Errorf("load live_map: %w", err)
	}

	engine := m.newBlockIOEngine()
	engine.deferred = ap
	result, err := engine.writeDeferred(name, vol, p, off, liveMap)
	if err != nil {
		m.mu.Unlock()
		return nil, result.Bytes, err
	}
	if result.AllocationBytesDelta != 0 {
		if vol.AllocatedBlocks < 0 {
			vol.AllocatedBlocks = 0
		}
		vol.AllocatedBlocks += result.AllocationBytesDelta / int64(vol.BlockSize)
		if vol.AllocatedBlocks < 0 {
			vol.AllocatedBlocks = 0
		}
		if data, merr := marshalVolume(vol); merr == nil {
			_, metaCommit, _ := ap.PutObjectAsync(context.Background(), volumeBucketName, metaKey(name), bytes.NewReader(data), "application/protobuf")
			result.CommitFns = append(result.CommitFns, metaCommit)
		}
	}

	m.mu.Unlock()
	return result.CommitFns, result.Bytes, nil
}

// Discard marks the byte range [off, off+length) as free.
// Only blocks fully within the range are deleted; partially covered blocks are skipped.
// This operation is idempotent — deleting non-existent blocks is not an error.
func (m *Manager) Discard(name string, off, length int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	vol, err := m.getVolUnlocked(name)
	if err != nil {
		return err
	}

	liveMap, err := m.getLiveMapUnlocked(name)
	if err != nil {
		return fmt.Errorf("load live_map: %w", err)
	}

	result, err := m.newBlockIOEngine().discard(name, vol, off, length, liveMap)
	if err != nil {
		return err
	}
	return m.applyBlockIOResultUnlocked(name, vol, result, liveMap, false, true, false)
}

// RecordFreedBytes decrements AllocatedBlocks by the equivalent number of blocks for freed bytes.
// This is used by higher-level layers (e.g., VFS) that manage file objects separately from
// block objects, but still want to keep storage accounting consistent.
// No-op when the volume is untracked (AllocatedBlocks < 0) or bytes == 0.
func (m *Manager) RecordFreedBytes(name string, n int64) error {
	if n <= 0 {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	vol, err := m.getVolUnlocked(name)
	if err != nil || vol.AllocatedBlocks <= 0 {
		return err
	}

	bs := int64(vol.BlockSize)
	freed := (n + bs - 1) / bs // ceil(n / blockSize) blocks
	if freed > vol.AllocatedBlocks {
		freed = vol.AllocatedBlocks
	}
	vol.AllocatedBlocks -= freed

	data, err := marshalVolume(vol)
	if err != nil {
		return err
	}
	m.backend.PutObject(context.Background(), volumeBucketName, metaKey(name), bytes.NewReader(data), "application/protobuf") //nolint:errcheck
	return nil
}

// Recalculate counts actual block objects via ListObjects and rewrites AllocatedBlocks.
// Returns (before, after int64, err error). If before==after no write is performed.
// AllocatedBlocks=-1 (untracked) is treated as drift — Recalculate initializes tracking.
func (m *Manager) Recalculate(name string) (int64, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	vol, err := m.getVolUnlocked(name)
	if err != nil {
		return 0, 0, err
	}

	var after int64
	if err := m.backend.WalkObjects(context.Background(), volumeBucketName, blockPrefix(name), func(_ *storage.Object) error {
		after++
		return nil
	}); err != nil {
		return 0, 0, fmt.Errorf("list blocks for recalculate: %w", err)
	}

	before := vol.AllocatedBlocks
	if before == after {
		return before, after, nil
	}

	vol.AllocatedBlocks = after
	data, err := marshalVolume(vol)
	if err != nil {
		return 0, 0, fmt.Errorf("marshal volume metadata: %w", err)
	}
	if _, err := m.backend.PutObject(context.Background(), volumeBucketName, metaKey(name), bytes.NewReader(data), "application/protobuf"); err != nil {
		return 0, 0, fmt.Errorf("store volume metadata: %w", err)
	}
	return before, after, nil
}

// getVolUnlocked returns the cached volume pointer (caller must hold m.mu).
// On cache miss, loads from storage and populates the cache.
func (m *Manager) getVolUnlocked(name string) (*Volume, error) {
	if vol, ok := m.volumes[name]; ok {
		return vol, nil
	}
	rc, _, err := m.backend.GetObject(context.Background(), volumeBucketName, metaKey(name))
	if err != nil {
		return nil, fmt.Errorf("volume %q: %w", name, ErrNotFound)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read volume metadata: %w", err)
	}

	vol, err := unmarshalVolume(data)
	if err != nil {
		return nil, fmt.Errorf("unmarshal volume metadata: %w", err)
	}
	m.volumes[name] = vol
	return vol, nil
}

func metaKey(name string) string {
	return metaPrefix + name + "/meta"
}

func blockKey(name string, blockNum int64) string {
	return fmt.Sprintf("%s%s/blk_%012d", metaPrefix, name, blockNum)
}

func blockPrefix(name string) string {
	return metaPrefix + name + "/blk_"
}

func (m *Manager) ensureBucket() error {
	_ = m.backend.CreateBucket(context.Background(), volumeBucketName)
	return m.backend.HeadBucket(context.Background(), volumeBucketName)
}

// --- live_map helpers ---

func liveMapKey(name string) string {
	return metaPrefix + name + "/live_map"
}

func snapPrefix(name string) string {
	return metaPrefix + name + "/snaps/"
}

func snapMapKey(name, snapID string) string {
	return metaPrefix + name + "/snaps/" + snapID + "/map"
}

func snapMetaKey(name, snapID string) string {
	return metaPrefix + name + "/snaps/" + snapID + "/meta"
}

func snapBlockKey(name, snapID string, blkNum int64) string {
	return fmt.Sprintf("%s%s/snaps/%s/blk_%012d", metaPrefix, name, snapID, blkNum)
}

// cowBlockKey returns a new versioned physical key for CoW writes.
func cowBlockKey(name string, blkNum int64) string {
	return fmt.Sprintf("%s%s/blk_%012d_v%s", metaPrefix, name, blkNum, uuid.Must(uuid.NewV7()).String())
}

// physicalKey resolves the physical object key for a logical block.
// If liveMap is nil or has no entry for blkNum, the default key is returned.
func physicalKey(name string, blkNum int64, liveMap map[int64]string) string {
	if liveMap != nil {
		if key, ok := liveMap[blkNum]; ok && key != "" {
			return key
		}
	}
	return blockKey(name, blkNum)
}

// getLiveMapUnlocked loads (or returns cached) the live_map for a volume.
// Returns nil when the volume has no snapshots, or when dedup is active (block
// mappings live in BadgerDB instead of the S3 live_map).
// Caller must hold m.mu.
func (m *Manager) getLiveMapUnlocked(name string) (map[int64]string, error) {
	if lm, ok := m.liveMaps[name]; ok {
		return lm, nil // nil cached entry = no live_map for this volume
	}
	vol, err := m.getVolUnlocked(name)
	if err != nil {
		return nil, err
	}
	// When dedup is active, block mappings live in BadgerDB (vd:b: prefix).
	// S3 live_map is not used; snapshots are incompatible with dedup in Phase A.
	if m.dedup != nil {
		if vol.SnapshotCount > 0 {
			return nil, errors.New("dedup + snapshots not supported in Phase A")
		}
		m.liveMaps[name] = nil
		return nil, nil
	}
	if vol.SnapshotCount == 0 {
		m.liveMaps[name] = nil
		return nil, nil
	}
	rc, _, err := m.backend.GetObject(context.Background(), volumeBucketName, liveMapKey(name))
	if err != nil {
		// No live_map object yet (fresh volume or fresh snapshot)
		lm := make(map[int64]string)
		m.liveMaps[name] = lm
		return lm, nil
	}
	defer rc.Close()
	lm, err := parseLiveMap(rc)
	if err != nil {
		return nil, fmt.Errorf("parse live_map: %w", err)
	}
	m.liveMaps[name] = lm
	return lm, nil
}

// persistLiveMapUnlocked writes the live_map to storage. Caller must hold m.mu.
func (m *Manager) persistLiveMapUnlocked(name string, lm map[int64]string) error {
	var buf bytes.Buffer
	if err := serializeLiveMap(lm, &buf); err != nil {
		return err
	}
	_, err := m.backend.PutObject(context.Background(), volumeBucketName, liveMapKey(name), &buf, "text/plain")
	return err
}

// parseLiveMap reads tab-separated "{blkNum}\t{physicalKey}\n" lines.
func parseLiveMap(r io.Reader) (map[int64]string, error) {
	lm := make(map[int64]string)
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid live_map line: %q", line)
		}
		blkNum, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid block number in live_map: %q", parts[0])
		}
		lm[blkNum] = parts[1]
	}
	return lm, scanner.Err()
}

// serializeLiveMap writes the live_map in tab-separated format.
func serializeLiveMap(lm map[int64]string, w io.Writer) error {
	bw := bufio.NewWriter(w)
	for blkNum, key := range lm {
		if _, err := fmt.Fprintf(bw, "%d\t%s\n", blkNum, key); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// snapshotMetaJSON is the JSON representation of snapshot metadata.
type snapshotMetaJSON struct {
	ID         string    `json:"id"`
	CreatedAt  time.Time `json:"created_at"`
	BlockCount int64     `json:"block_count"`
}

// --- Snapshot API ---

// CreateSnapshot creates a point-in-time snapshot of the named volume.
func (m *Manager) CreateSnapshot(name string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	vol, err := m.getVolUnlocked(name)
	if err != nil {
		return "", err
	}
	snapID, err := m.snapStore.CreateSnapshot(context.Background(), vol)
	if err != nil {
		return "", err
	}
	vol.SnapshotCount++
	data, err := marshalVolume(vol)
	if err != nil {
		return "", fmt.Errorf("marshal volume: %w", err)
	}
	if _, err := m.backend.PutObject(context.Background(), volumeBucketName, metaKey(name), bytes.NewReader(data), "application/protobuf"); err != nil {
		return "", fmt.Errorf("persist volume meta: %w", err)
	}
	return snapID, nil
}

// ListSnapshots returns all snapshots for the named volume.
func (m *Manager) ListSnapshots(name string) ([]SnapshotInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.getVolUnlocked(name); err != nil {
		return nil, err
	}
	return m.snapStore.ListSnapshots(context.Background(), name)
}

func (m *Manager) listSnapshotsUnlocked(name string) ([]SnapshotInfo, error) {
	if _, err := m.getVolUnlocked(name); err != nil {
		return nil, err
	}
	return m.snapStore.ListSnapshots(context.Background(), name)
}

// DeleteSnapshot removes a snapshot and frees its block objects.
func (m *Manager) DeleteSnapshot(name, snapID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.deleteSnapshotUnlocked(name, snapID)
}

func (m *Manager) deleteSnapshotUnlocked(name, snapID string) error {
	vol, err := m.getVolUnlocked(name)
	if err != nil {
		return err
	}
	if err := m.snapStore.DeleteSnapshot(context.Background(), vol, snapID); err != nil {
		return err
	}
	if vol.SnapshotCount > 0 {
		vol.SnapshotCount--
	}
	if vol.SnapshotCount == 0 {
		m.backend.DeleteObject(context.Background(), volumeBucketName, liveMapKey(name)) //nolint:errcheck
		delete(m.liveMaps, name)
	}
	data, err := marshalVolume(vol)
	if err != nil {
		return fmt.Errorf("marshal volume: %w", err)
	}
	_, err = m.backend.PutObject(context.Background(), volumeBucketName, metaKey(name), bytes.NewReader(data), "application/protobuf")
	return err
}

// Rollback restores the volume's block state to the given snapshot.
func (m *Manager) Rollback(name, snapID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	vol, err := m.getVolUnlocked(name)
	if err != nil {
		return err
	}
	return m.snapStore.Rollback(context.Background(), vol, snapID)
}

// Clone creates a new volume that initially shares blocks with the source volume.
func (m *Manager) Clone(srcName, dstName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	srcVol, err := m.getVolUnlocked(srcName)
	if err != nil {
		return fmt.Errorf("source volume: %w", err)
	}
	dstVol, err := m.snapStore.Clone(context.Background(), srcVol, dstName)
	if err != nil {
		return err
	}
	data, err := marshalVolume(dstVol)
	if err != nil {
		return fmt.Errorf("marshal dst volume: %w", err)
	}
	if _, err := m.backend.PutObject(context.Background(), volumeBucketName, metaKey(dstName), bytes.NewReader(data), "application/protobuf"); err != nil {
		return fmt.Errorf("store dst volume meta: %w", err)
	}
	m.volumes[dstName] = dstVol
	m.liveMaps[dstName] = nil
	return nil
}

// copyObjectFallback copies by reading source and writing to destination.
func (m *Manager) copyObjectFallback(srcBucket, srcKey, dstBucket, dstKey string) error {
	rc, obj, err := m.backend.GetObject(context.Background(), srcBucket, srcKey)
	if err != nil {
		return err
	}
	defer rc.Close()
	ct := "application/octet-stream"
	if obj != nil && obj.ContentType != "" {
		ct = obj.ContentType
	}
	_, err = m.backend.PutObject(context.Background(), dstBucket, dstKey, rc, ct)
	return err
}

// parseBlockNum extracts the block number from a block key like "__vol/name/blk_000000000042".
// Returns (blkNum, true) on success, (0, false) if not a block key.
func parseBlockNum(key string) (int64, bool) {
	// Find the last "/blk_" segment
	idx := strings.LastIndex(key, "/blk_")
	if idx < 0 {
		return 0, false
	}
	suffix := key[idx+5:] // after "/blk_"
	// Take the first 12 digits (zero-padded block number); ignore version suffix
	numStr := suffix
	if len(numStr) > 12 {
		numStr = numStr[:12]
	}
	n, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}
