package cluster

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

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
	// syncOnAppend is intentionally false by default. The shard pack is a hot
	// path for small EC shards; per-record fsync collapses throughput.
	syncOnAppend bool
}

func newShardPackStore(dir string) (*shardPackStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &shardPackStore{
		dir:       dir,
		maxSize:   shardPackMaxSize,
		index:     make(map[string]shardPackLocation),
		readFiles: make(map[uint64]*os.File),
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
	if s.activeOff > 0 && s.activeOff+entrySize > s.maxSize {
		if err := s.rotate(); err != nil {
			return err
		}
	}

	payloadOffset := s.activeOff + 9 + int64(len(key))
	if _, err := s.active.Write(record); err != nil {
		return err
	}
	if s.syncOnAppend {
		if err := s.active.Sync(); err != nil {
			return err
		}
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
	if s.active != nil {
		if err := s.active.Sync(); err != nil {
			return err
		}
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
		flag := header[4]
		dataLen := binary.BigEndian.Uint32(header[5:9])
		entrySize := int64(len(header)) + int64(keyLen) + int64(dataLen) + 4
		if entrySize > s.maxSize {
			return nil
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(f, key); err != nil {
			return nil
		}
		payloadOffset := off + int64(len(header)) + int64(keyLen)
		data := make([]byte, dataLen)
		if _, err := io.ReadFull(f, data); err != nil {
			return nil
		}
		var crcBuf [4]byte
		if _, err := io.ReadFull(f, crcBuf[:]); err != nil {
			return nil
		}
		off += entrySize
		if binary.BigEndian.Uint32(crcBuf[:]) != shardPackCRC(flag, string(key), data) {
			return nil
		}
		pkey := string(key)
		if flag == shardPackFlagPut {
			s.index[pkey] = shardPackLocation{blobID: blobID, offset: payloadOffset, length: dataLen}
		} else if flag == shardPackFlagDel {
			for ikey := range s.index {
				if strings.HasPrefix(ikey, pkey) {
					delete(s.index, ikey)
				}
			}
		}
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
