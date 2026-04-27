package volume

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gritive/GrainFS/internal/storage"
)

// ErrNotFound is returned when a volume does not exist.
var ErrNotFound = errors.New("volume not found")

const (
	DefaultBlockSize  = 4096
	volumeBucketName  = "__grainfs_volumes"
	metaPrefix        = "__vol/"
	maxBlockListLimit = 1_000_000
)

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
}

// SnapshotInfo describes a point-in-time snapshot of a volume.
type SnapshotInfo struct {
	ID         string
	CreatedAt  time.Time
	BlockCount int64 // number of allocated blocks at snapshot creation time
}

// Manager manages volumes on top of a storage.Backend.
type Manager struct {
	backend  storage.Backend
	mu       sync.Mutex                  // 단일 뮤텍스: read-modify-write 원자성 보장
	volumes  map[string]*Volume          // 인메모리 캐시
	liveMaps map[string]map[int64]string // cache: absent=未読み込み, nil entry=live_map 없음
	opts     ManagerOptions
}

// NewManager creates a new volume manager.
func NewManager(backend storage.Backend) *Manager {
	return NewManagerWithOptions(backend, ManagerOptions{})
}

// NewManagerWithOptions creates a new volume manager with the given options.
func NewManagerWithOptions(backend storage.Backend, opts ManagerOptions) *Manager {
	return &Manager{
		backend:  backend,
		volumes:  make(map[string]*Volume),
		liveMaps: make(map[string]map[int64]string),
		opts:     opts,
	}
}

// Create creates a new volume with the given name and size in bytes.
func (m *Manager) Create(name string, sizeBytes int64) (*Volume, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.ensureBucket(); err != nil {
		return nil, fmt.Errorf("ensure volume bucket: %w", err)
	}

	// Check if volume already exists
	if _, _, err := m.backend.GetObject(volumeBucketName, metaKey(name)); err == nil {
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

	if _, err := m.backend.PutObject(volumeBucketName, metaKey(name), bytes.NewReader(data), "application/protobuf"); err != nil {
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

// Delete deletes a volume and all its blocks.
func (m *Manager) Delete(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Verify volume exists
	if _, _, err := m.backend.GetObject(volumeBucketName, metaKey(name)); err != nil {
		return fmt.Errorf("volume %q: %w", name, ErrNotFound)
	}

	// Delete all block objects
	objs, err := m.backend.ListObjects(volumeBucketName, blockPrefix(name), maxBlockListLimit)
	if err == nil {
		for _, obj := range objs {
			_ = m.backend.DeleteObject(volumeBucketName, obj.Key)
		}
	}

	// Delete metadata
	if err := m.backend.DeleteObject(volumeBucketName, metaKey(name)); err != nil {
		return err
	}
	delete(m.volumes, name)
	return nil
}

// List returns all volumes.
func (m *Manager) List() ([]*Volume, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.backend.HeadBucket(volumeBucketName); err != nil {
		return nil, nil // no volumes bucket yet
	}

	objs, err := m.backend.ListObjects(volumeBucketName, metaPrefix, 10000)
	if err != nil {
		return nil, fmt.Errorf("list volume metadata: %w", err)
	}

	var volumes []*Volume
	for _, obj := range objs {
		if !strings.HasSuffix(obj.Key, "/meta") {
			continue
		}
		rc, _, err := m.backend.GetObject(volumeBucketName, obj.Key)
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

	bs := int64(vol.BlockSize)
	totalRead := 0

	for totalRead < len(p) && off+int64(totalRead) < vol.Size {
		pos := off + int64(totalRead)
		blkNum := pos / bs
		blkOff := pos % bs

		// Read the block
		blkData := make([]byte, vol.BlockSize)
		rc, _, err := m.backend.GetObject(volumeBucketName, physicalKey(name, blkNum, liveMap))
		if err != nil {
			// Block doesn't exist = zeros
			clear(blkData)
		} else {
			n, _ := io.ReadFull(rc, blkData)
			rc.Close()
			if n < vol.BlockSize {
				clear(blkData[n:])
			}
		}

		// Copy from block to output
		canRead := int(bs - blkOff)
		remaining := len(p) - totalRead
		if canRead > remaining {
			canRead = remaining
		}
		endPos := off + int64(totalRead) + int64(canRead)
		if endPos > vol.Size {
			canRead = int(vol.Size - pos)
		}

		copy(p[totalRead:totalRead+canRead], blkData[blkOff:blkOff+int64(canRead)])
		totalRead += canRead
	}

	return totalRead, nil
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

	liveMap, err := m.getLiveMapUnlocked(name)
	if err != nil {
		return 0, fmt.Errorf("load live_map: %w", err)
	}

	bs := int64(vol.BlockSize)
	useCow := vol.SnapshotCount > 0

	// Stage 1: PoolQuota pre-scan (only when quota is configured)
	if m.opts.PoolQuota > 0 {
		newBlocksNeeded := int64(0)
		firstBlk := off / bs
		lastBlk := (off + int64(len(p)) - 1) / bs
		if lastBlk >= vol.Size/bs {
			lastBlk = vol.Size/bs - 1
		}
		for blkNum := firstBlk; blkNum <= lastBlk; blkNum++ {
			if _, err := m.backend.HeadObject(volumeBucketName, physicalKey(name, blkNum, liveMap)); err != nil {
				newBlocksNeeded++
			}
		}

		currentAllocated := int64(0)
		for _, v := range m.volumes {
			if v.AllocatedBlocks > 0 {
				currentAllocated += v.AllocatedBlocks * int64(v.BlockSize)
			}
		}

		if currentAllocated+newBlocksNeeded*bs > m.opts.PoolQuota {
			return 0, ErrPoolQuotaExceeded
		}
	}

	// Stage 2: write loop
	totalWritten := 0
	newBlocks := 0
	liveMapDirty := false

	for totalWritten < len(p) && off+int64(totalWritten) < vol.Size {
		pos := off + int64(totalWritten)
		blkNum := pos / bs
		blkOff := pos % bs

		oldKey := physicalKey(name, blkNum, liveMap)

		// Read existing block (or zeros); isNew tracks if this is a new allocation
		blkData := make([]byte, vol.BlockSize)
		rc, _, readErr := m.backend.GetObject(volumeBucketName, oldKey)
		isNew := readErr != nil
		if !isNew {
			io.ReadFull(rc, blkData)
			rc.Close()
		}

		// Write into the block buffer
		canWrite := int(bs - blkOff)
		remaining := len(p) - totalWritten
		if canWrite > remaining {
			canWrite = remaining
		}
		endPos := off + int64(totalWritten) + int64(canWrite)
		if endPos > vol.Size {
			canWrite = int(vol.Size - pos)
		}

		copy(blkData[blkOff:blkOff+int64(canWrite)], p[totalWritten:totalWritten+canWrite])

		// Determine the target key: CoW allocates a new versioned key
		var targetKey string
		if useCow {
			targetKey = cowBlockKey(name, blkNum)
		} else {
			targetKey = blockKey(name, blkNum)
		}

		// Write the block
		if _, err := m.backend.PutObject(volumeBucketName, targetKey,
			bytes.NewReader(blkData), "application/octet-stream"); err != nil {
			return totalWritten, fmt.Errorf("write block %d: %w", blkNum, err)
		}

		// CoW: delete old physical object (PackedBackend decrements ref; deletes at ref=0)
		if useCow && oldKey != targetKey {
			if !isNew {
				m.backend.DeleteObject(volumeBucketName, oldKey) //nolint:errcheck
			}
			liveMap[blkNum] = targetKey
			liveMapDirty = true
		}

		if isNew {
			newBlocks++
		}
		totalWritten += canWrite
	}

	if liveMapDirty {
		if err := m.persistLiveMapUnlocked(name, liveMap); err != nil {
			return totalWritten, fmt.Errorf("persist live_map: %w", err)
		}
	}

	if newBlocks > 0 {
		if vol.AllocatedBlocks < 0 {
			vol.AllocatedBlocks = 0 // untracked → start tracking
		}
		vol.AllocatedBlocks += int64(newBlocks)
		data, err := marshalVolume(vol)
		if err == nil {
			m.backend.PutObject(volumeBucketName, metaKey(name), bytes.NewReader(data), "application/protobuf") //nolint:errcheck
		}
		// vol은 캐시 포인터이므로 캐시도 이미 갱신됨
	}

	return totalWritten, nil
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

	bs := int64(vol.BlockSize)
	firstBlock := (off + bs - 1) / bs // ceil(off/bs)
	lastBlock := (off+length)/bs - 1  // floor((off+length)/bs) - 1

	if lastBlock < firstBlock {
		return nil // no fully-covered blocks
	}

	liveMap, err := m.getLiveMapUnlocked(name)
	if err != nil {
		return fmt.Errorf("load live_map: %w", err)
	}

	freed := int64(0)
	for blkNum := firstBlock; blkNum <= lastBlock; blkNum++ {
		if err := m.backend.DeleteObject(volumeBucketName, physicalKey(name, blkNum, liveMap)); err == nil {
			freed++
			if liveMap != nil {
				delete(liveMap, blkNum)
			}
		}
	}
	if freed > 0 && liveMap != nil {
		_ = m.persistLiveMapUnlocked(name, liveMap)
	}

	if freed > 0 && vol.AllocatedBlocks >= 0 {
		if vol.AllocatedBlocks-freed < 0 {
			vol.AllocatedBlocks = 0
		} else {
			vol.AllocatedBlocks -= freed
		}
		data, err := marshalVolume(vol)
		if err == nil {
			m.backend.PutObject(volumeBucketName, metaKey(name), bytes.NewReader(data), "application/protobuf")
		}
	}
	return nil
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
	m.backend.PutObject(volumeBucketName, metaKey(name), bytes.NewReader(data), "application/protobuf") //nolint:errcheck
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

	objs, err := m.backend.ListObjects(volumeBucketName, blockPrefix(name), maxBlockListLimit)
	if err != nil {
		return 0, 0, fmt.Errorf("list blocks for recalculate: %w", err)
	}

	after := int64(len(objs))
	before := vol.AllocatedBlocks
	if before == after {
		return before, after, nil
	}

	vol.AllocatedBlocks = after
	data, err := marshalVolume(vol)
	if err != nil {
		return 0, 0, fmt.Errorf("marshal volume metadata: %w", err)
	}
	if _, err := m.backend.PutObject(volumeBucketName, metaKey(name), bytes.NewReader(data), "application/protobuf"); err != nil {
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
	rc, _, err := m.backend.GetObject(volumeBucketName, metaKey(name))
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
	_ = m.backend.CreateBucket(volumeBucketName)
	return m.backend.HeadBucket(volumeBucketName)
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
// Returns nil when the volume has no snapshots (zero overhead hot path).
// Caller must hold m.mu.
func (m *Manager) getLiveMapUnlocked(name string) (map[int64]string, error) {
	if lm, ok := m.liveMaps[name]; ok {
		return lm, nil // nil cached entry = no live_map for this volume
	}
	vol, err := m.getVolUnlocked(name)
	if err != nil {
		return nil, err
	}
	if vol.SnapshotCount == 0 {
		m.liveMaps[name] = nil
		return nil, nil
	}
	rc, _, err := m.backend.GetObject(volumeBucketName, liveMapKey(name))
	if err != nil {
		// No live_map object yet (fresh snapshot on volume with no previous writes)
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
	_, err := m.backend.PutObject(volumeBucketName, liveMapKey(name), &buf, "text/plain")
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

	liveMap, err := m.getLiveMapUnlocked(name)
	if err != nil {
		return "", fmt.Errorf("load live_map: %w", err)
	}

	snapID := uuid.Must(uuid.NewV7()).String()

	// Build snapshot_map from current live state
	snapMap := make(map[int64]string)
	if liveMap == nil || len(liveMap) == 0 {
		// No live_map: enumerate default block objects
		objs, err := m.backend.ListObjects(volumeBucketName, blockPrefix(name), maxBlockListLimit)
		if err != nil {
			return "", fmt.Errorf("list blocks: %w", err)
		}
		for _, obj := range objs {
			blkNum, ok := parseBlockNum(obj.Key)
			if !ok {
				continue
			}
			snapMap[blkNum] = obj.Key
		}
	} else {
		for k, v := range liveMap {
			snapMap[k] = v
		}
	}

	// CopyObject each block into snapshot namespace
	copier, hasCopier := m.backend.(storage.Copier)
	for blkNum, srcKey := range snapMap {
		dstKey := snapBlockKey(name, snapID, blkNum)
		if hasCopier {
			if _, err := copier.CopyObject(volumeBucketName, srcKey, volumeBucketName, dstKey); err != nil {
				return "", fmt.Errorf("copy block %d to snapshot: %w", blkNum, err)
			}
		} else {
			if err := m.copyObjectFallback(volumeBucketName, srcKey, volumeBucketName, dstKey); err != nil {
				return "", fmt.Errorf("copy block %d to snapshot: %w", blkNum, err)
			}
		}
		snapMap[blkNum] = dstKey
	}

	// Persist snapshot map
	var mapBuf bytes.Buffer
	if err := serializeLiveMap(snapMap, &mapBuf); err != nil {
		return "", fmt.Errorf("serialize snapshot map: %w", err)
	}
	if _, err := m.backend.PutObject(volumeBucketName, snapMapKey(name, snapID), &mapBuf, "text/plain"); err != nil {
		return "", fmt.Errorf("store snapshot map: %w", err)
	}

	// Persist snapshot metadata
	meta := snapshotMetaJSON{ID: snapID, CreatedAt: time.Now().UTC(), BlockCount: int64(len(snapMap))}
	metaData, err := json.Marshal(meta)
	if err != nil {
		return "", fmt.Errorf("marshal snapshot meta: %w", err)
	}
	if _, err := m.backend.PutObject(volumeBucketName, snapMetaKey(name, snapID), bytes.NewReader(metaData), "application/json"); err != nil {
		return "", fmt.Errorf("store snapshot meta: %w", err)
	}

	// Increment snapshot_count and persist volume metadata
	vol.SnapshotCount++
	data, err := marshalVolume(vol)
	if err != nil {
		return "", fmt.Errorf("marshal volume: %w", err)
	}
	if _, err := m.backend.PutObject(volumeBucketName, metaKey(name), bytes.NewReader(data), "application/protobuf"); err != nil {
		return "", fmt.Errorf("persist volume meta: %w", err)
	}

	// Invalidate live_map cache so next operation reloads
	delete(m.liveMaps, name)

	return snapID, nil
}

// ListSnapshots returns all snapshots for the named volume.
func (m *Manager) ListSnapshots(name string) ([]SnapshotInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, err := m.getVolUnlocked(name); err != nil {
		return nil, err
	}

	prefix := snapPrefix(name)
	objs, err := m.backend.ListObjects(volumeBucketName, prefix, maxBlockListLimit)
	if err != nil {
		return nil, fmt.Errorf("list snapshots: %w", err)
	}

	var snaps []SnapshotInfo
	for _, obj := range objs {
		if !strings.HasSuffix(obj.Key, "/meta") {
			continue
		}
		rc, _, err := m.backend.GetObject(volumeBucketName, obj.Key)
		if err != nil {
			continue
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			continue
		}
		var meta snapshotMetaJSON
		if err := json.Unmarshal(data, &meta); err != nil {
			continue
		}
		snaps = append(snaps, SnapshotInfo{ID: meta.ID, CreatedAt: meta.CreatedAt, BlockCount: meta.BlockCount})
	}
	return snaps, nil
}

// DeleteSnapshot removes a snapshot and frees its block objects.
func (m *Manager) DeleteSnapshot(name, snapID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	vol, err := m.getVolUnlocked(name)
	if err != nil {
		return err
	}

	// Load snapshot map to find blocks to delete
	rc, _, err := m.backend.GetObject(volumeBucketName, snapMapKey(name, snapID))
	if err != nil {
		return fmt.Errorf("snapshot %q not found for volume %q", snapID, name)
	}
	snapMap, err := parseLiveMap(rc)
	rc.Close()
	if err != nil {
		return fmt.Errorf("parse snapshot map: %w", err)
	}

	// Delete snapshot block objects (PackedBackend decrements ref; actual delete at ref=0)
	for _, key := range snapMap {
		m.backend.DeleteObject(volumeBucketName, key) //nolint:errcheck
	}
	m.backend.DeleteObject(volumeBucketName, snapMapKey(name, snapID))  //nolint:errcheck
	m.backend.DeleteObject(volumeBucketName, snapMetaKey(name, snapID)) //nolint:errcheck

	// Decrement snapshot_count and persist
	if vol.SnapshotCount > 0 {
		vol.SnapshotCount--
	}

	if vol.SnapshotCount == 0 {
		// All snapshots gone — delete live_map and reset to default-key mode
		m.backend.DeleteObject(volumeBucketName, liveMapKey(name)) //nolint:errcheck
		delete(m.liveMaps, name)
	}

	data, err := marshalVolume(vol)
	if err != nil {
		return fmt.Errorf("marshal volume: %w", err)
	}
	_, err = m.backend.PutObject(volumeBucketName, metaKey(name), bytes.NewReader(data), "application/protobuf")
	return err
}

// Rollback restores the volume's block state to the given snapshot.
func (m *Manager) Rollback(name, snapID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, err := m.getVolUnlocked(name); err != nil {
		return err
	}

	// Load snapshot map
	rc, _, err := m.backend.GetObject(volumeBucketName, snapMapKey(name, snapID))
	if err != nil {
		return fmt.Errorf("snapshot %q not found for volume %q", snapID, name)
	}
	snapMap, err := parseLiveMap(rc)
	rc.Close()
	if err != nil {
		return fmt.Errorf("parse snapshot map: %w", err)
	}

	// Load current live_map (create empty if needed)
	liveMap, err := m.getLiveMapUnlocked(name)
	if err != nil {
		return fmt.Errorf("load live_map: %w", err)
	}
	if liveMap == nil {
		liveMap = make(map[int64]string)
		m.liveMaps[name] = liveMap
	}

	copier, hasCopier := m.backend.(storage.Copier)

	// For each block in snapshot, copy back into live namespace with a new versioned key
	for blkNum, snapKey := range snapMap {
		newKey := cowBlockKey(name, blkNum)
		if hasCopier {
			if _, err := copier.CopyObject(volumeBucketName, snapKey, volumeBucketName, newKey); err != nil {
				return fmt.Errorf("rollback block %d: %w", blkNum, err)
			}
		} else {
			if err := m.copyObjectFallback(volumeBucketName, snapKey, volumeBucketName, newKey); err != nil {
				return fmt.Errorf("rollback block %d: %w", blkNum, err)
			}
		}

		// Delete the old live physical object
		if oldKey, ok := liveMap[blkNum]; ok && oldKey != "" {
			m.backend.DeleteObject(volumeBucketName, oldKey) //nolint:errcheck
		}
		liveMap[blkNum] = newKey
	}

	// Also delete any live blocks NOT in the snapshot (they shouldn't be read after rollback)
	for blkNum, oldKey := range liveMap {
		if _, inSnap := snapMap[blkNum]; !inSnap {
			m.backend.DeleteObject(volumeBucketName, oldKey) //nolint:errcheck
			delete(liveMap, blkNum)
		}
	}

	return m.persistLiveMapUnlocked(name, liveMap)
}

// Clone creates a new volume that initially shares blocks with the source volume.
func (m *Manager) Clone(srcName, dstName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	srcVol, err := m.getVolUnlocked(srcName)
	if err != nil {
		return fmt.Errorf("source volume: %w", err)
	}

	if err := m.ensureBucket(); err != nil {
		return fmt.Errorf("ensure bucket: %w", err)
	}

	// Fail if dst already exists
	if _, _, err := m.backend.GetObject(volumeBucketName, metaKey(dstName)); err == nil {
		return fmt.Errorf("volume %q already exists", dstName)
	}

	srcLiveMap, err := m.getLiveMapUnlocked(srcName)
	if err != nil {
		return fmt.Errorf("load live_map: %w", err)
	}

	copier, hasCopier := m.backend.(storage.Copier)

	doCopy := func(srcKey, dstKey string) error {
		if hasCopier {
			_, err := copier.CopyObject(volumeBucketName, srcKey, volumeBucketName, dstKey)
			return err
		}
		return m.copyObjectFallback(volumeBucketName, srcKey, volumeBucketName, dstKey)
	}

	if srcLiveMap != nil && len(srcLiveMap) > 0 {
		// Case 1: source has a live_map — copy each physical block to default keys in dst
		for blkNum, srcKey := range srcLiveMap {
			dstKey := blockKey(dstName, blkNum)
			if err := doCopy(srcKey, dstKey); err != nil {
				return fmt.Errorf("clone block %d: %w", blkNum, err)
			}
		}
	} else {
		// Case 2: no live_map — enumerate default blk_N objects
		objs, err := m.backend.ListObjects(volumeBucketName, blockPrefix(srcName), maxBlockListLimit)
		if err != nil {
			return fmt.Errorf("list source blocks: %w", err)
		}
		for _, obj := range objs {
			blkNum, ok := parseBlockNum(obj.Key)
			if !ok {
				continue
			}
			dstKey := blockKey(dstName, blkNum)
			if err := doCopy(obj.Key, dstKey); err != nil {
				return fmt.Errorf("clone block %d: %w", blkNum, err)
			}
		}
	}

	// Create dst volume metadata (SnapshotCount=0, no live_map)
	dstVol := &Volume{
		Name:            dstName,
		Size:            srcVol.Size,
		BlockSize:       srcVol.BlockSize,
		AllocatedBlocks: srcVol.AllocatedBlocks,
		SnapshotCount:   0,
	}
	data, err := marshalVolume(dstVol)
	if err != nil {
		return fmt.Errorf("marshal dst volume: %w", err)
	}
	if _, err := m.backend.PutObject(volumeBucketName, metaKey(dstName), bytes.NewReader(data), "application/protobuf"); err != nil {
		return fmt.Errorf("store dst volume meta: %w", err)
	}

	m.volumes[dstName] = dstVol
	m.liveMaps[dstName] = nil // dst starts with no live_map
	return nil
}

// copyObjectFallback copies by reading source and writing to destination.
func (m *Manager) copyObjectFallback(srcBucket, srcKey, dstBucket, dstKey string) error {
	rc, obj, err := m.backend.GetObject(srcBucket, srcKey)
	if err != nil {
		return err
	}
	defer rc.Close()
	ct := "application/octet-stream"
	if obj != nil && obj.ContentType != "" {
		ct = obj.ContentType
	}
	_, err = m.backend.PutObject(dstBucket, dstKey, rc, ct)
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
