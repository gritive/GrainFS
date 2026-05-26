package volume

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gritive/GrainFS/internal/cache/blockcache"
	"github.com/gritive/GrainFS/internal/pool"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/rs/zerolog/log"
)

// ErrNotFound is returned when a volume does not exist.
var ErrNotFound = errors.New("volume not found")

var volumeTraceEnabled = os.Getenv("GRAINFS_VOLUME_TRACE") == "1"

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

	// BlockCache, if non-nil, sits in front of backend.GetObject on
	// the ReadAt path. Hits skip the storage backend entirely; misses
	// populate the cache so subsequent reads of the same physical
	// block are free. Pass `blockcache.New(0)` (or leave nil) to
	// disable. Sizing guidance is in the blockcache package doc.
	BlockCache *blockcache.Cache
}

// Manager manages volumes on top of a storage.Backend.
type Manager struct {
	backend storage.Backend
	mu      sync.Mutex         // mutation mutex: protects volume metadata cache and ReadAt/WriteAt block-object consistency. Splitting this requires per-volume/per-block immutable versions or transactions.
	volumes map[string]*Volume // 인메모리 캐시
	blocks  *blockcache.Cache  // nil = block cache 비활성. ReadAt이 backend 앞에 두는 LRU.
	opts    ManagerOptions
	blkPool *pool.Pool[[]byte] // reusable DefaultBlockSize-byte slices for ReadAt/WriteAt
}

// NewManager creates a new volume manager.
func NewManager(backend storage.Backend) *Manager {
	return NewManagerWithOptions(backend, ManagerOptions{})
}

// NewManagerWithOptions creates a new volume manager with the given options.
func NewManagerWithOptions(backend storage.Backend, opts ManagerOptions) *Manager {
	return &Manager{
		backend: backend,
		volumes: make(map[string]*Volume),
		blocks:  opts.BlockCache,
		opts:    opts,
		blkPool: pool.New(func() []byte { return make([]byte, DefaultBlockSize) }),
	}
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

func (m *Manager) applyBlockIOResultUnlocked(name string, vol *Volume, result blockIOResult, strictMetadata, trackUntracked bool) error {
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

func (m *Manager) deleteUnlocked(name string) error {
	// Verify volume exists
	if _, _, err := m.backend.GetObject(context.Background(), volumeBucketName, metaKey(name)); err != nil {
		return fmt.Errorf("volume %q: %w", name, ErrNotFound)
	}

	// Delete all block objects.
	_ = m.backend.WalkObjects(context.Background(), volumeBucketName, blockPrefix(name), func(obj *storage.Object) error {
		_ = m.backend.DeleteObject(context.Background(), volumeBucketName, obj.Key)
		return nil
	})

	if err := m.backend.DeleteObject(context.Background(), volumeBucketName, metaKey(name)); err != nil {
		return err
	}
	delete(m.volumes, name)
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

	result, err := m.newBlockIOEngine().read(name, vol, p, off)
	return result.Bytes, err
}

// WriteAt writes len(p) bytes to the volume starting at byte offset off.
func (m *Manager) WriteAt(name string, p []byte, off int64) (int, error) {
	tStart := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if volumeTraceEnabled {
		defer func() {
			log.Debug().Str("volume", name).Int("bytes", len(p)).Dur("total", time.Since(tStart)).Msg("Volume WriteAt trace")
		}()
	}

	vol, err := m.getVolUnlocked(name)
	if err != nil {
		return 0, err
	}

	if off < 0 || off >= vol.Size {
		return 0, fmt.Errorf("offset %d out of range [0, %d)", off, vol.Size)
	}

	result, err := m.newBlockIOEngine().write(name, vol, p, off, m.currentAllocatedBytesUnlocked(), m.opts.PoolQuota)
	if err != nil {
		return result.Bytes, err
	}
	if err := m.applyBlockIOResultUnlocked(name, vol, result, false, true); err != nil {
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
	tStart := time.Now()
	ap, ok := m.backend.(asyncPutter)
	if !ok {
		n, err := m.WriteAt(name, p, off)
		return nil, n, err
	}
	if volumeTraceEnabled {
		defer func() {
			log.Debug().Str("volume", name).Int("bytes", len(p)).Dur("total", time.Since(tStart)).Msg("Volume WriteAtDeferred trace")
		}()
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

	// Async path only supports the no-quota fallback.
	if m.opts.PoolQuota > 0 {
		m.mu.Unlock()
		n, err := m.WriteAt(name, p, off)
		return nil, n, err
	}

	engine := m.newBlockIOEngine()
	engine.deferred = ap
	result, err := engine.writeDeferred(name, vol, p, off)
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

	result, err := m.newBlockIOEngine().discard(name, vol, off, length)
	if err != nil {
		return err
	}
	return m.applyBlockIOResultUnlocked(name, vol, result, true, false)
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
