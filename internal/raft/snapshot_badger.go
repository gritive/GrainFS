package raft

import (
	"encoding/binary"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

// badgerSnapshotStore persists Raft snapshots in BadgerDB under a caller-
// supplied prefix. PR 15 stores at most one snapshot at a time under the
// single key prefix+"latest"; Save replaces the prior snapshot atomically.
//
// Encoding (after the 1-byte version prefix):
//
//	[Version: 1B = 0x03]  // bumped from 0x02 for canonical snapshot metadata
//	[LastIncludedIndex: 8B BE]
//	[LastIncludedTerm:  8B BE]
//	[NumVoters: 4B BE]
//	  Repeated NumVoters times:
//	    [VoterIDLen: 4B BE]
//	    [VoterID: VoterIDLen bytes]
//	[NumLearners: 4B BE]  // M6.0 addition (Path B)
//	  Repeated NumLearners times:
//	    [LearnerIDLen:   2B BE][LearnerID]
//	    [LearnerAddrLen: 2B BE][LearnerAddr]
//	[FormatVersion: 1B]
//	[JointPhase: 1B]
//	[JointEnterIndex: 8B BE]
//	[NumJointOldVoters: 4B BE][string...]
//	[NumJointNewVoters: 4B BE][string...]
//	[NumJointManagedLearners: 4B BE][string...]
//	[DataLen: 8B BE]
//	[Data: DataLen bytes]
//
// Pre-M6.0 snapshots (Version 0x01) are refused on load — the logstore
// schema-version gate handles the migration story (clean re-bootstrap).
//
// Durability: Save calls db.Sync() after the write transaction commits, so
// the snapshot is power-loss durable on return. badger.DefaultOptions sets
// SyncWrites=false; the explicit Sync() makes the SnapshotStore safe even
// when the DB is shared with components that do not need per-write durability
// (mirrors badgerStableStore's approach).
type badgerSnapshotStore struct {
	db        *badger.DB
	keyPrefix []byte // copied at construction; immutable
}

const (
	snapshotEncodingVersion       byte = 0x03
	snapshotEncodingVersionLegacy byte = 0x02
	snapshotChunkedVersion        byte = 0x83
	snapshotChunkSize                  = 8 << 20
)

// newBadgerSnapshotStore opens a SnapshotStore view into db under prefix.
// Multiple Raft groups can coexist in one Badger DB with distinct prefixes.
func newBadgerSnapshotStore(db *badger.DB, prefix []byte) (*badgerSnapshotStore, error) {
	p := make([]byte, len(prefix))
	copy(p, prefix)
	return &badgerSnapshotStore{db: db, keyPrefix: p}, nil
}

// latestKey returns the single key under which the snapshot blob lives.
func (s *badgerSnapshotStore) latestKey() []byte {
	const suffix = "latest"
	key := make([]byte, len(s.keyPrefix)+len(suffix))
	copy(key, s.keyPrefix)
	copy(key[len(s.keyPrefix):], suffix)
	return key
}

func (s *badgerSnapshotStore) chunkKey(id []byte, ordinal uint32) []byte {
	const sep = "/"
	const suffix = "chunk/"
	key := make([]byte, len(s.keyPrefix)+len(suffix)+len(id)+len(sep)+4)
	off := 0
	copy(key[off:], s.keyPrefix)
	off += len(s.keyPrefix)
	copy(key[off:], suffix)
	off += len(suffix)
	copy(key[off:], id)
	off += len(id)
	copy(key[off:], sep)
	off += len(sep)
	binary.BigEndian.PutUint32(key[off:], ordinal)
	return key
}

// Latest returns the most recent saved snapshot, or (nil, nil) when no
// snapshot has been persisted yet. A decode failure (bad version, truncated
// blob) is surfaced so the caller can refuse to start with a corrupt
// snapshot rather than silently treating it as empty.
func (s *badgerSnapshotStore) Latest() (*Snapshot, error) {
	var snap *Snapshot
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(s.latestKey())
		if err == badger.ErrKeyNotFound {
			return nil // nothing saved yet
		}
		if err != nil {
			return fmt.Errorf("badgerSnapshotStore: Latest: %w", err)
		}
		return item.Value(func(val []byte) error {
			if len(val) > 0 && val[0] == snapshotChunkedVersion {
				manifest, decErr := decodeSnapshotChunkManifest(val)
				if decErr != nil {
					return decErr
				}
				chunked, readErr := s.readChunkedSnapshot(txn, manifest)
				if readErr != nil {
					return readErr
				}
				val = chunked
			}
			decoded, decErr := decodeSnapshot(val)
			if decErr != nil {
				return decErr
			}
			snap = decoded
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return snap, nil
}

// Save persists snap. Returns once the write is power-loss durable.
func (s *badgerSnapshotStore) Save(snap *Snapshot) error {
	if snap == nil {
		return fmt.Errorf("badgerSnapshotStore: Save: nil snapshot")
	}
	normalized := normalizedSnapshot(*snap)
	val := encodeSnapshot(&normalized)
	if len(val) > snapshotChunkSize {
		return s.saveChunkedSnapshot(&normalized, val)
	}
	oldManifest, hasOldManifest, err := s.latestChunkManifest()
	if err != nil {
		return err
	}
	err = s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(s.latestKey(), val)
	})
	if err != nil {
		return fmt.Errorf("badgerSnapshotStore: Save: %w", err)
	}
	if err := s.db.Sync(); err != nil {
		return fmt.Errorf("badgerSnapshotStore: Save sync: %w", err)
	}
	if hasOldManifest {
		if err := s.deleteSnapshotChunks(oldManifest); err != nil {
			return err
		}
	}
	return nil
}

type snapshotChunkManifest struct {
	totalLen  uint64
	chunkSize uint32
	numChunks uint32
	id        []byte
}

func (s *badgerSnapshotStore) saveChunkedSnapshot(snap *Snapshot, val []byte) error {
	oldManifest, hasOldManifest, err := s.latestChunkManifest()
	if err != nil {
		return err
	}
	id := snapshotChunkID(snap)
	numChunks := uint32((len(val) + snapshotChunkSize - 1) / snapshotChunkSize)
	manifest := snapshotChunkManifest{
		totalLen:  uint64(len(val)),
		chunkSize: snapshotChunkSize,
		numChunks: numChunks,
		id:        id,
	}
	for i := uint32(0); i < numChunks; i++ {
		start := int(i) * snapshotChunkSize
		end := start + snapshotChunkSize
		if end > len(val) {
			end = len(val)
		}
		chunk := val[start:end]
		err := s.db.Update(func(txn *badger.Txn) error {
			return txn.Set(s.chunkKey(id, i), chunk)
		})
		if err != nil {
			return fmt.Errorf("badgerSnapshotStore: Save chunk %d/%d: %w", i+1, numChunks, err)
		}
	}
	if err := s.db.Sync(); err != nil {
		return fmt.Errorf("badgerSnapshotStore: Save chunk sync: %w", err)
	}
	manifestVal := encodeSnapshotChunkManifest(manifest)
	err = s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(s.latestKey(), manifestVal)
	})
	if err != nil {
		return fmt.Errorf("badgerSnapshotStore: Save manifest: %w", err)
	}
	if err := s.db.Sync(); err != nil {
		return fmt.Errorf("badgerSnapshotStore: Save sync: %w", err)
	}
	if hasOldManifest && string(oldManifest.id) != string(id) {
		if err := s.deleteSnapshotChunks(oldManifest); err != nil {
			return err
		}
	}
	return nil
}

func (s *badgerSnapshotStore) latestChunkManifest() (snapshotChunkManifest, bool, error) {
	var manifest snapshotChunkManifest
	var found bool
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(s.latestKey())
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return fmt.Errorf("badgerSnapshotStore: latest manifest: %w", err)
		}
		return item.Value(func(val []byte) error {
			if len(val) == 0 || val[0] != snapshotChunkedVersion {
				return nil
			}
			decoded, err := decodeSnapshotChunkManifest(val)
			if err != nil {
				return err
			}
			manifest = decoded
			found = true
			return nil
		})
	})
	if err != nil {
		return snapshotChunkManifest{}, false, err
	}
	return manifest, found, nil
}

func (s *badgerSnapshotStore) deleteSnapshotChunks(manifest snapshotChunkManifest) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		for i := uint32(0); i < manifest.numChunks; i++ {
			if err := txn.Delete(s.chunkKey(manifest.id, i)); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("badgerSnapshotStore: delete old chunks: %w", err)
	}
	if err := s.db.Sync(); err != nil {
		return fmt.Errorf("badgerSnapshotStore: delete old chunks sync: %w", err)
	}
	return nil
}

func snapshotChunkID(snap *Snapshot) []byte {
	id := make([]byte, 16)
	binary.BigEndian.PutUint64(id[:8], snap.LastIncludedIndex)
	binary.BigEndian.PutUint64(id[8:], snap.LastIncludedTerm)
	return id
}

func encodeSnapshotChunkManifest(manifest snapshotChunkManifest) []byte {
	size := 1 + 8 + 4 + 4 + 2 + len(manifest.id)
	buf := make([]byte, size)
	off := 0
	buf[off] = snapshotChunkedVersion
	off++
	binary.BigEndian.PutUint64(buf[off:], manifest.totalLen)
	off += 8
	binary.BigEndian.PutUint32(buf[off:], manifest.chunkSize)
	off += 4
	binary.BigEndian.PutUint32(buf[off:], manifest.numChunks)
	off += 4
	binary.BigEndian.PutUint16(buf[off:], uint16(len(manifest.id)))
	off += 2
	copy(buf[off:], manifest.id)
	return buf
}

func decodeSnapshotChunkManifest(val []byte) (snapshotChunkManifest, error) {
	if len(val) < 1+8+4+4+2 {
		return snapshotChunkManifest{}, fmt.Errorf("badgerSnapshotStore: short chunk manifest")
	}
	if val[0] != snapshotChunkedVersion {
		return snapshotChunkManifest{}, fmt.Errorf("badgerSnapshotStore: unknown chunk manifest version 0x%02x", val[0])
	}
	off := 1
	totalLen := binary.BigEndian.Uint64(val[off:])
	off += 8
	chunkSize := binary.BigEndian.Uint32(val[off:])
	off += 4
	numChunks := binary.BigEndian.Uint32(val[off:])
	off += 4
	idLen := int(binary.BigEndian.Uint16(val[off:]))
	off += 2
	if len(val) != off+idLen {
		return snapshotChunkManifest{}, fmt.Errorf("badgerSnapshotStore: invalid chunk manifest id length")
	}
	if chunkSize == 0 || numChunks == 0 {
		return snapshotChunkManifest{}, fmt.Errorf("badgerSnapshotStore: invalid chunk manifest geometry")
	}
	id := make([]byte, idLen)
	copy(id, val[off:])
	return snapshotChunkManifest{
		totalLen:  totalLen,
		chunkSize: chunkSize,
		numChunks: numChunks,
		id:        id,
	}, nil
}

func (s *badgerSnapshotStore) readChunkedSnapshot(txn *badger.Txn, manifest snapshotChunkManifest) ([]byte, error) {
	if manifest.totalLen > uint64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("badgerSnapshotStore: chunked snapshot too large")
	}
	buf := make([]byte, int(manifest.totalLen))
	off := 0
	for i := uint32(0); i < manifest.numChunks; i++ {
		item, err := txn.Get(s.chunkKey(manifest.id, i))
		if err != nil {
			return nil, fmt.Errorf("badgerSnapshotStore: Latest chunk %d/%d: %w", i+1, manifest.numChunks, err)
		}
		err = item.Value(func(chunk []byte) error {
			if off+len(chunk) > len(buf) {
				return fmt.Errorf("badgerSnapshotStore: chunked snapshot length overflow")
			}
			copy(buf[off:], chunk)
			off += len(chunk)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	if off != len(buf) {
		return nil, fmt.Errorf("badgerSnapshotStore: chunked snapshot length mismatch (%d != %d)", off, len(buf))
	}
	return buf, nil
}

// encodeSnapshot serialises a Snapshot into the binary format documented
// on the type. The version prefix lets future encodings break old readers
// loudly rather than silently misinterpret the blob.
func encodeSnapshot(snap *Snapshot) []byte {
	normalized := normalizedSnapshot(*snap)
	snap = &normalized

	// Pre-compute size to allocate exactly once.
	size := 1 + 8 + 8 + 4 // version + lastIdx + lastTerm + numVoters
	for _, v := range snap.Configuration {
		size += 4 + len(v)
	}
	size += 4 // numLearners
	for id, addr := range snap.Learners {
		size += 2 + len(id) + 2 + len(addr)
	}
	size += 1 + 1 + 8 // FormatVersion + JointPhase + JointEnterIndex
	size += stringListEncodedSize(snap.JointOldVoters)
	size += stringListEncodedSize(snap.JointNewVoters)
	size += stringListEncodedSize(snap.JointManagedLearners)
	size += 8 + len(snap.Data) // dataLen + data
	buf := make([]byte, size)

	off := 0
	buf[off] = snapshotEncodingVersion
	off++
	binary.BigEndian.PutUint64(buf[off:], snap.LastIncludedIndex)
	off += 8
	binary.BigEndian.PutUint64(buf[off:], snap.LastIncludedTerm)
	off += 8
	binary.BigEndian.PutUint32(buf[off:], uint32(len(snap.Configuration)))
	off += 4
	for _, v := range snap.Configuration {
		binary.BigEndian.PutUint32(buf[off:], uint32(len(v)))
		off += 4
		copy(buf[off:], v)
		off += len(v)
	}
	binary.BigEndian.PutUint32(buf[off:], uint32(len(snap.Learners)))
	off += 4
	for id, addr := range snap.Learners {
		binary.BigEndian.PutUint16(buf[off:], uint16(len(id)))
		off += 2
		copy(buf[off:], id)
		off += len(id)
		binary.BigEndian.PutUint16(buf[off:], uint16(len(addr)))
		off += 2
		copy(buf[off:], addr)
		off += len(addr)
	}
	buf[off] = snap.FormatVersion
	off++
	buf[off] = byte(snap.JointPhase)
	off++
	binary.BigEndian.PutUint64(buf[off:], snap.JointEnterIndex)
	off += 8
	off = encodeStringList(buf, off, snap.JointOldVoters)
	off = encodeStringList(buf, off, snap.JointNewVoters)
	off = encodeStringList(buf, off, snap.JointManagedLearners)
	binary.BigEndian.PutUint64(buf[off:], uint64(len(snap.Data)))
	off += 8
	copy(buf[off:], snap.Data)
	return buf
}

func stringListEncodedSize(values []string) int {
	size := 4
	for _, value := range values {
		size += 4 + len(value)
	}
	return size
}

func encodeStringList(buf []byte, off int, values []string) int {
	binary.BigEndian.PutUint32(buf[off:], uint32(len(values)))
	off += 4
	for _, value := range values {
		binary.BigEndian.PutUint32(buf[off:], uint32(len(value)))
		off += 4
		copy(buf[off:], value)
		off += len(value)
	}
	return off
}

// decodeSnapshot is the inverse of encodeSnapshot. Errors on any structural
// problem (short buffer, unknown version) so the caller refuses to start
// from a corrupt snapshot.
func decodeSnapshot(val []byte) (*Snapshot, error) {
	if len(val) < 1 {
		return nil, fmt.Errorf("badgerSnapshotStore: empty value")
	}
	version := val[0]
	if version != snapshotEncodingVersion && version != snapshotEncodingVersionLegacy {
		return nil, fmt.Errorf("badgerSnapshotStore: unknown version 0x%02x", version)
	}
	off := 1
	if len(val) < off+8+8+4 {
		return nil, fmt.Errorf("badgerSnapshotStore: short header")
	}
	lastIdx := binary.BigEndian.Uint64(val[off:])
	off += 8
	lastTerm := binary.BigEndian.Uint64(val[off:])
	off += 8
	numVoters := int(binary.BigEndian.Uint32(val[off:]))
	off += 4
	var voters []string
	if numVoters > 0 {
		voters = make([]string, numVoters)
		for i := 0; i < numVoters; i++ {
			if len(val) < off+4 {
				return nil, fmt.Errorf("badgerSnapshotStore: short voter[%d] len", i)
			}
			vlen := int(binary.BigEndian.Uint32(val[off:]))
			off += 4
			if len(val) < off+vlen {
				return nil, fmt.Errorf("badgerSnapshotStore: short voter[%d] body", i)
			}
			voters[i] = string(val[off : off+vlen])
			off += vlen
		}
	}
	// Learners section (M6.0 v2 schema).
	if len(val) < off+4 {
		return nil, fmt.Errorf("badgerSnapshotStore: short numLearners")
	}
	numLearners := int(binary.BigEndian.Uint32(val[off:]))
	off += 4
	var learners map[string]string
	if numLearners > 0 {
		learners = make(map[string]string, numLearners)
		for i := 0; i < numLearners; i++ {
			if len(val) < off+2 {
				return nil, fmt.Errorf("badgerSnapshotStore: short learner[%d] id len", i)
			}
			idLen := int(binary.BigEndian.Uint16(val[off:]))
			off += 2
			if len(val) < off+idLen {
				return nil, fmt.Errorf("badgerSnapshotStore: short learner[%d] id", i)
			}
			id := string(val[off : off+idLen])
			off += idLen
			if len(val) < off+2 {
				return nil, fmt.Errorf("badgerSnapshotStore: short learner[%d] addr len", i)
			}
			addrLen := int(binary.BigEndian.Uint16(val[off:]))
			off += 2
			if len(val) < off+addrLen {
				return nil, fmt.Errorf("badgerSnapshotStore: short learner[%d] addr", i)
			}
			learners[id] = string(val[off : off+addrLen])
			off += addrLen
		}
	}
	formatVersion := FSMSnapshotFormatVersion
	jointPhase := JointNone
	var jointEnterIndex uint64
	var jointOldVoters []string
	var jointNewVoters []string
	var jointManagedLearners []string
	if version == snapshotEncodingVersion {
		if len(val) < off+1+1+8 {
			return nil, fmt.Errorf("badgerSnapshotStore: short snapshot metadata")
		}
		formatVersion = val[off]
		off++
		jointPhase = JointPhase(val[off])
		off++
		jointEnterIndex = binary.BigEndian.Uint64(val[off:])
		off += 8
		var err error
		jointOldVoters, off, err = decodeStringList(val, off, "JointOldVoters")
		if err != nil {
			return nil, err
		}
		jointNewVoters, off, err = decodeStringList(val, off, "JointNewVoters")
		if err != nil {
			return nil, err
		}
		jointManagedLearners, off, err = decodeStringList(val, off, "JointManagedLearners")
		if err != nil {
			return nil, err
		}
	}
	if len(val) < off+8 {
		return nil, fmt.Errorf("badgerSnapshotStore: short DataLen")
	}
	dataLen := int(binary.BigEndian.Uint64(val[off:]))
	off += 8
	if len(val) < off+dataLen {
		return nil, fmt.Errorf("badgerSnapshotStore: short Data body (%d < %d)", len(val)-off, dataLen)
	}
	var data []byte
	if dataLen > 0 {
		data = make([]byte, dataLen)
		copy(data, val[off:off+dataLen])
	}
	snap := normalizedSnapshot(Snapshot{
		LastIncludedIndex:    lastIdx,
		LastIncludedTerm:     lastTerm,
		FormatVersion:        uint8(formatVersion),
		JointPhase:           jointPhase,
		JointOldVoters:       jointOldVoters,
		JointNewVoters:       jointNewVoters,
		JointEnterIndex:      jointEnterIndex,
		JointManagedLearners: jointManagedLearners,
		Configuration:        voters,
		Learners:             learners,
		Data:                 data,
	})
	return &snap, nil
}

func decodeStringList(val []byte, off int, name string) ([]string, int, error) {
	if len(val) < off+4 {
		return nil, off, fmt.Errorf("badgerSnapshotStore: short %s count", name)
	}
	count := int(binary.BigEndian.Uint32(val[off:]))
	off += 4
	if count == 0 {
		return nil, off, nil
	}
	values := make([]string, count)
	for i := 0; i < count; i++ {
		if len(val) < off+4 {
			return nil, off, fmt.Errorf("badgerSnapshotStore: short %s[%d] len", name, i)
		}
		valueLen := int(binary.BigEndian.Uint32(val[off:]))
		off += 4
		if len(val) < off+valueLen {
			return nil, off, fmt.Errorf("badgerSnapshotStore: short %s[%d] body", name, i)
		}
		values[i] = string(val[off : off+valueLen])
		off += valueLen
	}
	return values, off, nil
}
