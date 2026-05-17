package packblob

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/gritive/GrainFS/internal/encrypt"
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
	entryOverhead  = 4 + 1 + 4 + 4 // key_len + flags + data_len + crc32
	flagCompressed = byte(0x01)
	flagEncrypted  = byte(0x02)
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
	encryptor *encrypt.Encryptor
	lockFile  *os.File // held for the lifetime of this BlobStore (flock on unix)

	// readFiles caches open *os.File handles for completed (read-only) blobs.
	// Published as an immutable map snapshot via atomic.Pointer so the hot
	// read path (BlobStore.Read → getReadFile) does not share bs.mu with
	// BlobStore.Append on the writer hot path. Insert is a CAS-retry CoW;
	// concurrent first-fillers race os.Open and the loser closes its
	// duplicate fd (acceptable: fills are rare — once per blob file).
	// Audit follow-up: lock-free-audit.md → "BlobStore.getReadFile shares
	// bs.mu with Append; separate when mixed-workload mutex profile shows
	// contention on the read path."
	readFiles atomic.Pointer[map[uint64]*os.File]
}

// EnableCompression enables zstd compression for new entries.
//
// Construction-only: must be called before the BlobStore is shared with any
// goroutine, including before the owning PackedBackend's periodic-save
// goroutine starts. After construction the bs.compress flag is read
// without the mutex from BlobStore.Append's pre-lock compression path; a
// concurrent write to this flag would race that read.
func (bs *BlobStore) EnableCompression() {
	bs.compress = true
}

// NewBlobStore creates a blob store rooted at dir.
func NewBlobStore(dir string, maxSize int64) (*BlobStore, error) {
	return newBlobStore(dir, maxSize)
}

// NewEncryptedBlobStore creates a blob store that encrypts entry payloads.
func NewEncryptedBlobStore(dir string, maxSize int64, enc *encrypt.Encryptor) (*BlobStore, error) {
	if enc == nil {
		return nil, fmt.Errorf("encrypted blob store requires encryptor")
	}
	bs, err := newBlobStore(dir, maxSize)
	if err != nil {
		return nil, err
	}
	bs.encryptor = enc
	return bs, nil
}

func newBlobStore(dir string, maxSize int64) (*BlobStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create blob dir: %w", err)
	}

	bs := &BlobStore{
		dir:     dir,
		maxSize: maxSize,
	}
	initial := make(map[uint64]*os.File)
	bs.readFiles.Store(&initial)

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
	// Compress before locking — CPU-bound, depends only on input. bs.compress
	// is set once at construction via EnableCompression and never mutated
	// thereafter, so this read is safe without the mutex. Moving compression
	// outside the critical section keeps zstd CPU off the serial writer path
	// (audit follow-up: lock-free-audit.md → "compression must stay outside
	// the critical section if it becomes visible in mutex profiles").
	flags := byte(0)
	payload := data
	if bs.compress {
		compressed, err := compress(data)
		if err == nil && len(compressed) < len(data) {
			payload = compressed
			flags = flagCompressed
		}
	}
	storedPayload := payload

	bs.mu.Lock()
	defer bs.mu.Unlock()

	offset := bs.activeOff
	if bs.encryptor != nil {
		flags |= flagEncrypted
		sealed, err := bs.encryptor.SealValueAADTo(nil, bs.entryAAD(bs.activeID, uint64(offset), key, flags), storedPayload)
		if err != nil {
			return BlobLocation{}, fmt.Errorf("encrypt blob entry: %w", err)
		}
		payload = sealed
	}

	entrySize := int64(entryOverhead + len(key) + len(payload))

	// Rotate if this entry would exceed max blob size
	if bs.activeOff+entrySize > bs.maxSize && bs.activeOff > 0 {
		if err := bs.rotate(); err != nil {
			return BlobLocation{}, err
		}
		offset = bs.activeOff
		if bs.encryptor != nil {
			sealed, err := bs.encryptor.SealValueAADTo(nil, bs.entryAAD(bs.activeID, uint64(offset), key, flags), storedPayload)
			if err != nil {
				return BlobLocation{}, fmt.Errorf("encrypt blob entry: %w", err)
			}
			payload = sealed
			entrySize = int64(entryOverhead + len(key) + len(payload))
		}
	}

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

	crc := make([]byte, 4)
	binary.BigEndian.PutUint32(crc, blobEntryCRC([]byte(key), flags, payload))
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
	f, err := bs.getReadFile(loc.BlobID)
	if err != nil {
		return nil, err
	}
	off := int64(loc.Offset)

	const maxEntrySize = 256 * 1024 * 1024 // 256MB sanity cap against corrupt blobs
	var header [4]byte

	// Read key_len
	if err := readFullAt(f, header[:], &off); err != nil {
		return nil, err
	}
	keyLen := binary.BigEndian.Uint32(header[:])
	if keyLen > maxEntrySize {
		return nil, fmt.Errorf("corrupt blob: keyLen %d exceeds max", keyLen)
	}

	restLen := int(keyLen) + 1 + 4 + int(loc.Length) + 4
	rest := make([]byte, restLen)
	if err := readFullAt(f, rest, &off); err != nil {
		return nil, err
	}

	key := rest[:keyLen]
	cursor := int(keyLen)

	flags := rest[cursor]
	cursor++

	dataLen := binary.BigEndian.Uint32(rest[cursor : cursor+4])
	cursor += 4
	if dataLen > maxEntrySize {
		return nil, fmt.Errorf("corrupt blob: dataLen %d exceeds max", dataLen)
	}
	if dataLen != loc.Length {
		return nil, fmt.Errorf("corrupt blob: dataLen %d does not match location length %d", dataLen, loc.Length)
	}

	payload := rest[cursor : cursor+int(dataLen)]
	cursor += int(dataLen)

	expectedCRC := binary.BigEndian.Uint32(rest[cursor : cursor+4])

	if !bs.skipReadCRC(flags) {
		if blobEntryCRC(key, flags, payload) != expectedCRC {
			if legacyBlobEntryCRC(key, payload) != expectedCRC {
				return nil, fmt.Errorf("CRC mismatch at blob %d offset %d", loc.BlobID, loc.Offset)
			}
			if err := bs.rejectEncryptedFlagDowngrade(loc.BlobID, loc.Offset, string(key), flags, payload); err != nil {
				return nil, err
			}
		}
	}

	payload, err = bs.decodePayload(loc.BlobID, loc.Offset, string(key), flags, payload)
	if err != nil {
		return nil, err
	}
	if flags&flagCompressed != 0 {
		return decompress(payload)
	}
	return payload, nil
}

// Close closes the active blob file, every cached read-fd, and releases
// the directory lock. All cleanup steps run unconditionally so a single
// failing Close() does not leak fds or strand the directory lock; the
// returned error is the joined set of every failure encountered.
//
// The caller must guarantee no concurrent Append / Read / getReadFile is
// in flight. Reads no longer take bs.mu (readFiles is published via
// atomic.Pointer), so a concurrent read started after Close has begun
// could insert a duplicate fd into the empty post-Close map and leak
// it. PackedBackend.Close is the only production caller and runs at
// shutdown after request handlers have drained.
func (bs *BlobStore) Close() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	var errs []error
	if bs.active != nil {
		if err := bs.active.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close active blob: %w", err))
		}
	}
	m := bs.readFiles.Load()
	for id, f := range *m {
		if err := f.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close read fd for blob %d: %w", id, err))
		}
	}
	empty := make(map[uint64]*os.File)
	bs.readFiles.Store(&empty)
	releaseBlobDirLock(bs.lockFile)
	return errors.Join(errs...)
}

func (bs *BlobStore) getReadFile(blobID uint64) (*os.File, error) {
	if f := (*bs.readFiles.Load())[blobID]; f != nil {
		return f, nil
	}
	path := bs.blobPath(blobID)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open blob %d: %w", blobID, err)
	}
	for {
		old := bs.readFiles.Load()
		if existing := (*old)[blobID]; existing != nil {
			_ = f.Close() // lost the race to another caller
			return existing, nil
		}
		next := make(map[uint64]*os.File, len(*old)+1)
		for k, v := range *old {
			next[k] = v
		}
		next[blobID] = f
		if bs.readFiles.CompareAndSwap(old, &next) {
			return f, nil
		}
	}
}

func readFullAt(f *os.File, buf []byte, off *int64) error {
	if len(buf) == 0 {
		return nil
	}
	if _, err := f.ReadAt(buf, *off); err != nil {
		return err
	}
	*off += int64(len(buf))
	return nil
}

func (bs *BlobStore) skipReadCRC(flags byte) bool {
	// AES-GCM authenticates encrypted blob entries, including key, blob id,
	// offset, and flags via AAD. Recomputing CRC32 over the ciphertext on every
	// GET duplicates that integrity check on the hot path.
	return bs.encryptor != nil && flags&flagEncrypted != 0
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
	var offset int64
	for {
		if _, err := io.ReadFull(f, header[:]); err != nil {
			break
		}
		entryOffset := offset
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

		if _, err := io.ReadFull(f, header[:]); err != nil {
			break
		}
		expectedCRC := binary.BigEndian.Uint32(header[:])
		offset += int64(entryOverhead + int(keyLen) + int(dataLen))

		if !tombstones[string(key)] {
			if blobEntryCRC(key, flags, payload) != expectedCRC {
				if legacyBlobEntryCRC(key, payload) != expectedCRC {
					return nil, fmt.Errorf("CRC mismatch at blob %d offset %d", blobID, entryOffset)
				}
				if err := bs.rejectEncryptedFlagDowngrade(blobID, uint64(entryOffset), string(key), flags, payload); err != nil {
					return nil, err
				}
			}
			data, err := bs.decodePayload(blobID, uint64(entryOffset), string(key), flags, payload)
			if err != nil {
				return nil, err
			}
			if flags&flagCompressed != 0 {
				data, err = decompress(data)
				if err != nil {
					return nil, err
				}
			}
			entries = append(entries, rawEntry{key: string(key), data: data})
		}
	}
	f.Close()

	// Force rotate to a new blob so compacted entries don't go into the same file
	bs.mu.Lock()
	if bs.activeID == blobID {
		if err := bs.rotate(); err != nil {
			bs.mu.Unlock()
			return nil, err
		}
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

func (bs *BlobStore) entryAAD(blobID uint64, offset uint64, key string, flags byte) []byte {
	keyBytes := []byte(key)
	aad := make([]byte, len("packblob:v2:")+8+8+1+4+len(keyBytes))
	copy(aad, "packblob:v2:")
	off := len("packblob:v2:")
	binary.BigEndian.PutUint64(aad[off:], blobID)
	off += 8
	binary.BigEndian.PutUint64(aad[off:], offset)
	off += 8
	aad[off] = flags
	off++
	binary.BigEndian.PutUint32(aad[off:], uint32(len(keyBytes)))
	off += 4
	copy(aad[off:], keyBytes)
	return aad
}

func blobEntryCRC(key []byte, flags byte, payload []byte) uint32 {
	h := crc32.NewIEEE()
	h.Write(key)
	_, _ = h.Write([]byte{flags})
	h.Write(payload)
	return h.Sum32()
}

func legacyBlobEntryCRC(key []byte, payload []byte) uint32 {
	h := crc32.NewIEEE()
	h.Write(key)
	h.Write(payload)
	return h.Sum32()
}

func (bs *BlobStore) rejectEncryptedFlagDowngrade(blobID uint64, offset uint64, key string, flags byte, payload []byte) error {
	if bs.encryptor == nil || flags&flagEncrypted != 0 || !encrypt.IsEncryptedValue(payload) {
		return nil
	}
	for _, candidate := range encryptedFlagCandidates(flags) {
		if _, err := bs.encryptor.OpenValueAAD(bs.entryAAD(blobID, offset, key, candidate), payload); err == nil {
			return fmt.Errorf("encrypted blob entry flags mismatch")
		}
	}
	return nil
}

func encryptedFlagCandidates(flags byte) []byte {
	withEncrypted := flags | flagEncrypted
	withCompressedEncrypted := flags | flagCompressed | flagEncrypted
	if withEncrypted == withCompressedEncrypted {
		return []byte{withEncrypted}
	}
	return []byte{withEncrypted, withCompressedEncrypted}
}

func (bs *BlobStore) decodePayload(blobID uint64, offset uint64, key string, flags byte, payload []byte) ([]byte, error) {
	if bs.encryptor == nil {
		return payload, nil
	}
	if flags&flagEncrypted != 0 {
		plain, err := bs.encryptor.OpenValueAAD(bs.entryAAD(blobID, offset, key, flags), payload)
		if err != nil {
			return nil, fmt.Errorf("decrypt blob entry: %w", err)
		}
		return plain, nil
	}
	if !encrypt.IsEncryptedValue(payload) {
		return payload, nil
	}
	if err := bs.rejectEncryptedFlagDowngrade(blobID, offset, key, flags, payload); err != nil {
		return nil, err
	}
	return payload, nil
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
			expectedCRC := binary.BigEndian.Uint32(header[:])
			if crc := blobEntryCRC(key, flagBuf[0], data); crc != expectedCRC && legacyBlobEntryCRC(key, data) != expectedCRC {
				offset += int64(entryOverhead + int(keyLen) + int(dataLen))
				continue
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
