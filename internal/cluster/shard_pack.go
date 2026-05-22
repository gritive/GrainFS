package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/gritive/GrainFS/internal/storage/datawal"
)

// errPackRecordCorrupt signals a CRC mismatch on a pack record. In scanFile
// (on-disk replay) this is treated as end-of-data (torn write): the rest of
// the blob is discarded. In appendRawRecord (WAL replay) it is a hard error:
// the WAL payload was bit-flipped between the original append and replay.
var errPackRecordCorrupt = errors.New("shard pack: record CRC mismatch")

const (
	shardPackMaxSize      = 256 << 20
	shardPackFlagPut byte = 1
	shardPackFlagDel byte = 2
)

type shardPackLocation struct {
	blobID uint64
	offset int64
	length uint32
}

type shardPackStore struct {
	dir       string
	maxSize   int64
	mu        sync.Mutex
	active    *os.File
	activeID  uint64
	activeOff int64
	nextID    uint64
	index     map[string]shardPackLocation
	readFiles map[uint64]*os.File
	scratch   []byte
	// dataWAL, when non-nil, receives an OpShardPackPut / OpShardPackDelete
	// record carrying the raw on-disk pack record bytes before the in-memory
	// pack blob is appended. RecoverDataWAL passes nil to avoid recursion
	// when materializing replayed records back into the pack.
	dataWAL DataWALAppender
}

func newShardPackStore(dir string, dwal DataWALAppender) (*shardPackStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &shardPackStore{
		dir:       dir,
		maxSize:   shardPackMaxSize,
		index:     make(map[string]shardPackLocation),
		readFiles: make(map[uint64]*os.File),
		dataWAL:   dwal,
	}
	if err := s.scan(); err != nil {
		return nil, err
	}
	if err := s.rotate(); err != nil {
		return nil, err
	}
	return s, nil
}

func shardPackKey(bucket, key string, shardIdx int) string {
	return bucket + "\x00" + key + "\x00" + strconv.Itoa(shardIdx)
}

func shardPackPrefix(bucket, key string) string {
	return bucket + "\x00" + key + "\x00"
}

func (s *shardPackStore) put(bucket, key string, shardIdx int, data []byte) error {
	return s.append(shardPackFlagPut, shardPackKey(bucket, key, shardIdx), data)
}

func (s *shardPackStore) deleteKey(bucket, key string) error {
	return s.append(shardPackFlagDel, shardPackPrefix(bucket, key), nil)
}

func (s *shardPackStore) get(bucket, key string, shardIdx int) ([]byte, bool, error) {
	pkey := shardPackKey(bucket, key, shardIdx)
	s.mu.Lock()
	loc, ok := s.index[pkey]
	f := s.readFiles[loc.blobID]
	if ok && f == nil && s.activeID == loc.blobID {
		f = s.active
	}
	if ok && f == nil {
		var err error
		f, err = os.Open(s.blobPath(loc.blobID))
		if err != nil {
			s.mu.Unlock()
			return nil, true, err
		}
		s.readFiles[loc.blobID] = f
	}
	s.mu.Unlock()
	if !ok {
		return nil, false, nil
	}
	data := make([]byte, loc.length)
	if _, err := f.ReadAt(data, loc.offset); err != nil {
		return nil, true, err
	}
	return data, true, nil
}

func (s *shardPackStore) append(flag byte, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.scratch = appendShardPackRecord(s.scratch[:0], flag, key, data)
	record := s.scratch
	entrySize := int64(len(record))

	// Log the raw on-disk record bytes to the data WAL before mutating the
	// pack blob so a torn write can be replayed verbatim by appendRawRecord.
	// Skipped when no WAL is wired and when called during recovery (where the
	// materializer constructs a nil-WAL store explicitly).
	if s.dataWAL != nil {
		op := datawal.OpShardPackPut
		if flag == shardPackFlagDel {
			op = datawal.OpShardPackDelete
		}
		if _, err := s.dataWAL.Append(context.Background(), datawal.Record{
			Op:      op,
			Target:  key,
			Size:    int64(len(record)),
			Payload: record,
		}); err != nil {
			return fmt.Errorf("data wal append shard pack: %w", err)
		}
		if err := s.dataWAL.Flush(); err != nil {
			return fmt.Errorf("data wal flush shard pack: %w", err)
		}
	}

	if s.activeOff > 0 && s.activeOff+entrySize > s.maxSize {
		if err := s.rotate(); err != nil {
			return err
		}
	}

	payloadOffset := s.activeOff + 9 + int64(len(key))
	if _, err := s.active.Write(record); err != nil {
		return err
	}
	s.activeOff += entrySize

	if flag == shardPackFlagPut {
		s.index[key] = shardPackLocation{blobID: s.activeID, offset: payloadOffset, length: uint32(len(data))}
	} else if flag == shardPackFlagDel {
		for ikey := range s.index {
			if strings.HasPrefix(ikey, key) {
				delete(s.index, ikey)
			}
		}
	}
	return nil
}

func appendShardPackRecord(dst []byte, flag byte, key string, data []byte) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, 4+1+4+len(key)+len(data)+4)...)
	record := dst[start:]
	binary.BigEndian.PutUint32(record[0:4], uint32(len(key)))
	record[4] = flag
	binary.BigEndian.PutUint32(record[5:9], uint32(len(data)))
	off := 9
	copy(record[off:], key)
	off += len(key)
	copy(record[off:], data)
	off += len(data)
	binary.BigEndian.PutUint32(record[off:], shardPackCRC(flag, key, data))
	return dst
}

func (s *shardPackStore) rotate() error {
	// Per-rotation fsync of the closing blob is intentionally omitted: the data
	// WAL owns durability for every pack record (see append above), so a
	// crash-then-replay rebuilds the rotated blob from WAL records. The dir
	// sync after open keeps the new blob's directory entry durable.
	if s.active != nil {
		if err := s.active.Close(); err != nil {
			return err
		}
	}
	id := s.nextID
	f, err := os.OpenFile(s.blobPath(id), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	s.active = f
	s.activeID = id
	s.activeOff = 0
	s.nextID = id + 1
	return syncDir(s.dir)
}

// Close releases the active pack file handle and any cached read handles.
// Used by RecoverDataWAL to drop the pre-recovery state before replaying
// records onto a freshly-opened store.
func (s *shardPackStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	if s.active != nil {
		if err := s.active.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.active = nil
	}
	for id, f := range s.readFiles {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(s.readFiles, id)
	}
	return firstErr
}

// appendRawRecord replays a WAL-captured pack record back onto the on-disk
// pack blob and rebuilds the in-memory index. The record bytes are exactly
// what append wrote in the original (pre-crash) call, so scanRecord parses
// them with the same CRC envelope used by scanFile.
func (s *shardPackStore) appendRawRecord(record []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(record) < 9+4 {
		return fmt.Errorf("shard pack: replay record too short: %d", len(record))
	}
	entrySize := int64(len(record))
	if s.active == nil {
		if err := s.rotate(); err != nil {
			return err
		}
	} else if s.activeOff > 0 && s.activeOff+entrySize > s.maxSize {
		if err := s.rotate(); err != nil {
			return err
		}
	}
	if _, err := s.active.Write(record); err != nil {
		return err
	}
	off := s.activeOff
	s.activeOff += entrySize
	return s.scanRecord(record, s.activeID, off)
}

// scanRecord parses one on-disk pack record and updates the in-memory index.
// Returns errPackRecordCorrupt on a CRC mismatch. Callers decide whether the
// mismatch is end-of-data (scanFile, torn-write convention) or a hard error
// (appendRawRecord, WAL replay).
func (s *shardPackStore) scanRecord(record []byte, blobID uint64, recordOffset int64) error {
	if len(record) < 9+4 {
		return fmt.Errorf("shard pack record too short: %d", len(record))
	}
	keyLen := binary.BigEndian.Uint32(record[0:4])
	flag := record[4]
	dataLen := binary.BigEndian.Uint32(record[5:9])
	if int64(9)+int64(keyLen)+int64(dataLen)+4 != int64(len(record)) {
		return fmt.Errorf("shard pack record length mismatch")
	}
	key := record[9 : 9+keyLen]
	data := record[9+keyLen : 9+keyLen+dataLen]
	crcGot := binary.BigEndian.Uint32(record[9+keyLen+dataLen:])
	if crcGot != shardPackCRC(flag, string(key), data) {
		return errPackRecordCorrupt
	}
	pkey := string(key)
	switch flag {
	case shardPackFlagPut:
		payloadOffset := recordOffset + 9 + int64(keyLen)
		s.index[pkey] = shardPackLocation{blobID: blobID, offset: payloadOffset, length: dataLen}
	case shardPackFlagDel:
		for ikey := range s.index {
			if strings.HasPrefix(ikey, pkey) {
				delete(s.index, ikey)
			}
		}
	}
	return nil
}

func (s *shardPackStore) scan() error {
	matches, err := filepath.Glob(filepath.Join(s.dir, "shardpack_*.dat"))
	if err != nil {
		return err
	}
	for _, path := range matches {
		id, ok := parseShardPackID(path)
		if !ok {
			continue
		}
		if id >= s.nextID {
			s.nextID = id + 1
		}
		if err := s.scanFile(id, path); err != nil {
			return err
		}
	}
	return nil
}

func (s *shardPackStore) scanFile(blobID uint64, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var off int64
	for {
		var header [9]byte
		if _, err := io.ReadFull(f, header[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		keyLen := binary.BigEndian.Uint32(header[0:4])
		dataLen := binary.BigEndian.Uint32(header[5:9])
		entrySize := int64(len(header)) + int64(keyLen) + int64(dataLen) + 4
		if entrySize > s.maxSize {
			return nil
		}
		record := make([]byte, entrySize)
		copy(record[:len(header)], header[:])
		if _, err := io.ReadFull(f, record[len(header):]); err != nil {
			return nil
		}
		if err := s.scanRecord(record, blobID, off); err != nil {
			if errors.Is(err, errPackRecordCorrupt) {
				// Torn write: discard the rest of the blob. Preserves the
				// pre-refactor scanFile semantics where the first CRC
				// mismatch ends the scan.
				return nil
			}
			return err
		}
		off += entrySize
	}
}

func parseShardPackID(path string) (uint64, bool) {
	var id uint64
	if _, err := fmt.Sscanf(filepath.Base(path), "shardpack_%016x.dat", &id); err != nil {
		return 0, false
	}
	return id, true
}

func (s *shardPackStore) blobPath(id uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("shardpack_%016x.dat", id))
}

func shardPackCRC(flag byte, key string, data []byte) uint32 {
	h := crc32.NewIEEE()
	_, _ = h.Write([]byte{flag})
	_, _ = h.Write([]byte(key))
	_, _ = h.Write(data)
	return h.Sum32()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
