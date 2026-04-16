package packblob

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// BlobLocation identifies where an entry lives within a blob file.
type BlobLocation struct {
	BlobID uint64 // which blob file
	Offset uint64 // byte offset within the blob
	Length uint32 // data length (excluding header/checksum)
}

// entryHeader is written before each data entry in the blob file.
// Format: [4 bytes key_len][key bytes][4 bytes data_len][data bytes][4 bytes CRC32]
const entryOverhead = 4 + 4 + 4 // key_len + data_len + crc32

// BlobStore manages append-only blob files for packing small objects.
type BlobStore struct {
	dir      string
	maxSize  int64
	mu       sync.Mutex
	active   *os.File
	activeID uint64
	activeOff int64
	nextID   atomic.Uint64
}

// NewBlobStore creates a blob store rooted at dir.
func NewBlobStore(dir string, maxSize int64) (*BlobStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create blob dir: %w", err)
	}

	bs := &BlobStore{
		dir:     dir,
		maxSize: maxSize,
	}
	bs.nextID.Store(1)

	if err := bs.rotate(); err != nil {
		return nil, err
	}
	return bs, nil
}

// Append writes a key+data entry to the active blob. Returns the location.
func (bs *BlobStore) Append(key string, data []byte) (BlobLocation, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	entrySize := int64(entryOverhead + len(key) + len(data))

	// Rotate if this entry would exceed max blob size
	if bs.activeOff+entrySize > bs.maxSize && bs.activeOff > 0 {
		if err := bs.rotate(); err != nil {
			return BlobLocation{}, err
		}
	}

	offset := bs.activeOff

	// Write: [key_len:4][key][data_len:4][data][crc32:4]
	keyLen := make([]byte, 4)
	binary.BigEndian.PutUint32(keyLen, uint32(len(key)))
	if _, err := bs.active.Write(keyLen); err != nil {
		return BlobLocation{}, err
	}
	if _, err := bs.active.Write([]byte(key)); err != nil {
		return BlobLocation{}, err
	}

	dataLen := make([]byte, 4)
	binary.BigEndian.PutUint32(dataLen, uint32(len(data)))
	if _, err := bs.active.Write(dataLen); err != nil {
		return BlobLocation{}, err
	}
	if _, err := bs.active.Write(data); err != nil {
		return BlobLocation{}, err
	}

	// CRC32 of key + data
	h := crc32.NewIEEE()
	h.Write([]byte(key))
	h.Write(data)
	crc := make([]byte, 4)
	binary.BigEndian.PutUint32(crc, h.Sum32())
	if _, err := bs.active.Write(crc); err != nil {
		return BlobLocation{}, err
	}

	bs.activeOff += entrySize

	return BlobLocation{
		BlobID: bs.activeID,
		Offset: uint64(offset),
		Length: uint32(len(data)),
	}, nil
}

// Read reads the data at the given location. Verifies CRC32.
func (bs *BlobStore) Read(loc BlobLocation) ([]byte, error) {
	path := bs.blobPath(loc.BlobID)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open blob %d: %w", loc.BlobID, err)
	}
	defer f.Close()

	if _, err := f.Seek(int64(loc.Offset), 0); err != nil {
		return nil, err
	}

	// Read key_len
	header := make([]byte, 4)
	if _, err := f.Read(header); err != nil {
		return nil, err
	}
	keyLen := binary.BigEndian.Uint32(header)

	// Read key
	key := make([]byte, keyLen)
	if _, err := f.Read(key); err != nil {
		return nil, err
	}

	// Read data_len
	if _, err := f.Read(header); err != nil {
		return nil, err
	}
	dataLen := binary.BigEndian.Uint32(header)

	// Read data
	data := make([]byte, dataLen)
	if dataLen > 0 {
		if _, err := f.Read(data); err != nil {
			return nil, err
		}
	}

	// Read and verify CRC
	if _, err := f.Read(header); err != nil {
		return nil, err
	}
	expectedCRC := binary.BigEndian.Uint32(header)

	h := crc32.NewIEEE()
	h.Write(key)
	h.Write(data)
	if h.Sum32() != expectedCRC {
		return nil, fmt.Errorf("CRC mismatch at blob %d offset %d", loc.BlobID, loc.Offset)
	}

	return data, nil
}

// Close closes the active blob file.
func (bs *BlobStore) Close() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if bs.active != nil {
		return bs.active.Close()
	}
	return nil
}

func (bs *BlobStore) rotate() error {
	if bs.active != nil {
		bs.active.Close()
	}

	id := bs.nextID.Add(1) - 1
	path := bs.blobPath(id)
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create blob file: %w", err)
	}

	bs.active = f
	bs.activeID = id
	bs.activeOff = 0
	return nil
}

// Compact rewrites a blob file, skipping tombstoned keys.
// Returns new locations for surviving entries. The old blob file
// is removed after the new one is written (unlink-while-open safe).
func (bs *BlobStore) Compact(blobID uint64, tombstones map[string]bool) (map[string]BlobLocation, error) {
	path := bs.blobPath(blobID)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open blob for compaction: %w", err)
	}
	defer f.Close()

	// Read all entries from the old blob
	type rawEntry struct {
		key  string
		data []byte
	}
	var entries []rawEntry

	for {
		header := make([]byte, 4)
		_, err := f.Read(header)
		if err != nil {
			break // EOF or error
		}
		keyLen := binary.BigEndian.Uint32(header)
		key := make([]byte, keyLen)
		if _, err := f.Read(key); err != nil {
			break
		}

		if _, err := f.Read(header); err != nil {
			break
		}
		dataLen := binary.BigEndian.Uint32(header)
		data := make([]byte, dataLen)
		if dataLen > 0 {
			if _, err := f.Read(data); err != nil {
				break
			}
		}

		// Skip CRC
		if _, err := f.Read(header); err != nil {
			break
		}

		if !tombstones[string(key)] {
			entries = append(entries, rawEntry{key: string(key), data: data})
		}
	}
	f.Close()

	// Force rotate to a new blob so compacted entries don't go into the same file
	bs.mu.Lock()
	if bs.activeID == blobID {
		bs.rotate()
	}
	bs.mu.Unlock()

	// Write surviving entries to the new active blob
	newLocs := make(map[string]BlobLocation)
	for _, e := range entries {
		loc, err := bs.Append(e.key, e.data)
		if err != nil {
			return nil, fmt.Errorf("compaction append: %w", err)
		}
		newLocs[e.key] = loc
	}

	// Remove old blob file (safe: no new reads will target it after index update)
	os.Remove(path)

	return newLocs, nil
}

func (bs *BlobStore) blobPath(id uint64) string {
	return filepath.Join(bs.dir, fmt.Sprintf("blob_%016x.blob", id))
}
