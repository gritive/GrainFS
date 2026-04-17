package packblob

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
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

// Entry format: [key_len:4][key][flags:1][data_len:4][data][crc32:4]
// flags bit 0: compressed (zstd)
const (
	entryOverhead   = 4 + 1 + 4 + 4 // key_len + flags + data_len + crc32
	flagCompressed  = byte(0x01)
)

// BlobStore manages append-only blob files for packing small objects.
type BlobStore struct {
	dir       string
	maxSize   int64
	mu        sync.Mutex
	active    *os.File
	activeID  uint64
	activeOff int64
	nextID    atomic.Uint64
	compress  bool
	lockFile  *os.File // held for the lifetime of this BlobStore (flock on unix)
}

// EnableCompression enables zstd compression for new entries.
func (bs *BlobStore) EnableCompression() {
	bs.compress = true
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

	// Acquire an exclusive directory lock to prevent two BlobStore instances
	// from writing the same blob files concurrently.
	lf, err := acquireBlobDirLock(dir)
	if err != nil {
		return nil, fmt.Errorf("lock blob dir: %w", err)
	}
	bs.lockFile = lf

	// Scan existing blob files to find the highest ID so we don't overwrite them on restart.
	maxID := uint64(0)
	matches, err := filepath.Glob(filepath.Join(dir, "blob_*.blob"))
	if err != nil {
		releaseBlobDirLock(lf)
		return nil, fmt.Errorf("scan blob dir: %w", err)
	}
	for _, p := range matches {
		var id uint64
		if _, err := fmt.Sscanf(filepath.Base(p), "blob_%016x.blob", &id); err == nil && id > maxID {
			maxID = id
		}
	}
	bs.nextID.Store(maxID + 1)

	if err := bs.rotate(); err != nil {
		releaseBlobDirLock(lf)
		return nil, err
	}
	return bs, nil
}

// Append writes a key+data entry to the active blob. Returns the location.
func (bs *BlobStore) Append(key string, data []byte) (BlobLocation, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	flags := byte(0)
	payload := data
	if bs.compress {
		compressed, err := compress(data)
		if err == nil && len(compressed) < len(data) {
			payload = compressed
			flags = flagCompressed
		}
	}

	entrySize := int64(entryOverhead + len(key) + len(payload))

	// Rotate if this entry would exceed max blob size
	if bs.activeOff+entrySize > bs.maxSize && bs.activeOff > 0 {
		if err := bs.rotate(); err != nil {
			return BlobLocation{}, err
		}
	}

	offset := bs.activeOff

	// Write: [key_len:4][key][flags:1][data_len:4][data][crc32:4]
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(key)))
	if _, err := bs.active.Write(header); err != nil {
		return BlobLocation{}, err
	}
	if _, err := bs.active.Write([]byte(key)); err != nil {
		return BlobLocation{}, err
	}
	if _, err := bs.active.Write([]byte{flags}); err != nil {
		return BlobLocation{}, err
	}
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	if _, err := bs.active.Write(header); err != nil {
		return BlobLocation{}, err
	}
	if _, err := bs.active.Write(payload); err != nil {
		return BlobLocation{}, err
	}

	// CRC32 of key + payload (compressed or raw)
	h := crc32.NewIEEE()
	h.Write([]byte(key))
	h.Write(payload)
	crc := make([]byte, 4)
	binary.BigEndian.PutUint32(crc, h.Sum32())
	if _, err := bs.active.Write(crc); err != nil {
		return BlobLocation{}, err
	}

	bs.activeOff += entrySize

	return BlobLocation{
		BlobID: bs.activeID,
		Offset: uint64(offset),
		Length: uint32(len(payload)),
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

	const maxEntrySize = 256 * 1024 * 1024 // 256MB sanity cap against corrupt blobs
	var header [4]byte

	// Read key_len
	if _, err := io.ReadFull(f, header[:]); err != nil {
		return nil, err
	}
	keyLen := binary.BigEndian.Uint32(header[:])
	if keyLen > maxEntrySize {
		return nil, fmt.Errorf("corrupt blob: keyLen %d exceeds max", keyLen)
	}

	// Read key
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(f, key); err != nil {
		return nil, err
	}

	// Read flags
	var flagBuf [1]byte
	if _, err := io.ReadFull(f, flagBuf[:]); err != nil {
		return nil, err
	}
	flags := flagBuf[0]

	// Read data_len
	if _, err := io.ReadFull(f, header[:]); err != nil {
		return nil, err
	}
	dataLen := binary.BigEndian.Uint32(header[:])
	if dataLen > maxEntrySize {
		return nil, fmt.Errorf("corrupt blob: dataLen %d exceeds max", dataLen)
	}

	// Read payload (may be compressed)
	payload := make([]byte, dataLen)
	if dataLen > 0 {
		if _, err := io.ReadFull(f, payload); err != nil {
			return nil, err
		}
	}

	// Read and verify CRC
	if _, err := io.ReadFull(f, header[:]); err != nil {
		return nil, err
	}
	expectedCRC := binary.BigEndian.Uint32(header[:])

	h := crc32.NewIEEE()
	h.Write(key)
	h.Write(payload)
	if h.Sum32() != expectedCRC {
		return nil, fmt.Errorf("CRC mismatch at blob %d offset %d", loc.BlobID, loc.Offset)
	}

	if flags&flagCompressed != 0 {
		return decompress(payload)
	}
	return payload, nil
}

// Close closes the active blob file and releases the directory lock.
func (bs *BlobStore) Close() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if bs.active != nil {
		if err := bs.active.Close(); err != nil {
			return err
		}
	}
	releaseBlobDirLock(bs.lockFile)
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

	const maxEntrySize = 256 * 1024 * 1024 // 256MB sanity cap against corrupt blobs
	var header [4]byte
	var flagBuf [1]byte
	for {
		if _, err := io.ReadFull(f, header[:]); err != nil {
			break
		}
		keyLen := binary.BigEndian.Uint32(header[:])
		if keyLen > maxEntrySize {
			break
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(f, key); err != nil {
			break
		}

		// Read flags byte
		if _, err := io.ReadFull(f, flagBuf[:]); err != nil {
			break
		}
		flags := flagBuf[0]

		if _, err := io.ReadFull(f, header[:]); err != nil {
			break
		}
		dataLen := binary.BigEndian.Uint32(header[:])
		if dataLen > maxEntrySize {
			break
		}
		payload := make([]byte, dataLen)
		if dataLen > 0 {
			if _, err := io.ReadFull(f, payload); err != nil {
				break
			}
		}

		// Skip CRC
		if _, err := io.ReadFull(f, header[:]); err != nil {
			break
		}

		if !tombstones[string(key)] {
			// Decompress before re-appending (Append will re-compress if needed)
			data := payload
			if flags&flagCompressed != 0 {
				if dec, err := decompress(payload); err == nil {
					data = dec
				}
			}
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

// ScanAll scans all blob files and returns the last known location for each key.
func (bs *BlobStore) ScanAll() (map[string]BlobLocation, error) {
	matches, err := filepath.Glob(filepath.Join(bs.dir, "blob_*.blob"))
	if err != nil {
		return nil, fmt.Errorf("glob blobs: %w", err)
	}

	locs := make(map[string]BlobLocation)
	for _, path := range matches {
		var blobID uint64
		if _, err := fmt.Sscanf(filepath.Base(path), "blob_%016x.blob", &blobID); err != nil {
			continue
		}

		f, err := os.Open(path)
		if err != nil {
			continue
		}

		const maxEntrySize = 256 * 1024 * 1024 // 256MB sanity cap
		var header [4]byte
		var flagBuf [1]byte
		var offset int64
		for {
			if _, err := io.ReadFull(f, header[:]); err != nil {
				break
			}
			keyLen := binary.BigEndian.Uint32(header[:])
			if keyLen > maxEntrySize {
				break
			}
			key := make([]byte, keyLen)
			if _, err := io.ReadFull(f, key); err != nil {
				break
			}

			if _, err := io.ReadFull(f, flagBuf[:]); err != nil {
				break
			}

			if _, err := io.ReadFull(f, header[:]); err != nil {
				break
			}
			dataLen := binary.BigEndian.Uint32(header[:])
			if dataLen > maxEntrySize {
				break
			}
			data := make([]byte, dataLen)
			if dataLen > 0 {
				if _, err := io.ReadFull(f, data); err != nil {
					break
				}
			}
			if _, err := io.ReadFull(f, header[:]); err != nil { // skip CRC
				break
			}

			entrySize := int64(entryOverhead + int(keyLen) + int(dataLen))
			locs[string(key)] = BlobLocation{
				BlobID: blobID,
				Offset: uint64(offset),
				Length: dataLen,
			}
			offset += entrySize
		}
		f.Close()
	}
	return locs, nil
}
