package volume

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/gritive/GrainFS/internal/storage"
)

const (
	DefaultBlockSize = 4096
	volumeBucketName = "__grainfs_volumes"
	metaPrefix       = "__vol/"
)

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
}

// Manager manages volumes on top of a storage.Backend.
type Manager struct {
	backend storage.Backend
	mu      sync.Mutex         // 단일 뮤텍스: read-modify-write 원자성 보장
	volumes map[string]*Volume // 인메모리 캐시
	opts    ManagerOptions
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
		opts:    opts,
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
		return fmt.Errorf("volume %q not found", name)
	}

	// Delete all block objects
	objs, err := m.backend.ListObjects(volumeBucketName, blockPrefix(name), 100000)
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

	bs := int64(vol.BlockSize)
	totalRead := 0

	for totalRead < len(p) && off+int64(totalRead) < vol.Size {
		pos := off + int64(totalRead)
		blkNum := pos / bs
		blkOff := pos % bs

		// Read the block
		blkData := make([]byte, vol.BlockSize)
		rc, _, err := m.backend.GetObject(volumeBucketName, blockKey(name, blkNum))
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

	bs := int64(vol.BlockSize)
	totalWritten := 0
	newBlocks := 0

	for totalWritten < len(p) && off+int64(totalWritten) < vol.Size {
		pos := off + int64(totalWritten)
		blkNum := pos / bs
		blkOff := pos % bs

		// Read existing block (or zeros); isNew tracks if this is a new allocation
		blkData := make([]byte, vol.BlockSize)
		rc, _, err := m.backend.GetObject(volumeBucketName, blockKey(name, blkNum))
		isNew := err != nil
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

		// Write the block back
		if _, err := m.backend.PutObject(volumeBucketName, blockKey(name, blkNum),
			bytes.NewReader(blkData), "application/octet-stream"); err != nil {
			return totalWritten, fmt.Errorf("write block %d: %w", blkNum, err)
		}

		if isNew {
			newBlocks++
		}
		totalWritten += canWrite
	}

	if newBlocks > 0 {
		if vol.AllocatedBlocks < 0 {
			vol.AllocatedBlocks = 0 // untracked → start tracking
		}
		vol.AllocatedBlocks += int64(newBlocks)
		data, err := marshalVolume(vol)
		if err == nil {
			m.backend.PutObject(volumeBucketName, metaKey(name), bytes.NewReader(data), "application/protobuf")
		}
		// vol은 캐시 포인터이므로 캐시도 이미 갱신됨
	}

	return totalWritten, nil
}

// getVolUnlocked returns the cached volume pointer (caller must hold m.mu).
// On cache miss, loads from storage and populates the cache.
func (m *Manager) getVolUnlocked(name string) (*Volume, error) {
	if vol, ok := m.volumes[name]; ok {
		return vol, nil
	}
	rc, _, err := m.backend.GetObject(volumeBucketName, metaKey(name))
	if err != nil {
		return nil, fmt.Errorf("volume %q not found", name)
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
