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
	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(s.latestKey(), val)
	})
	if err != nil {
		return fmt.Errorf("badgerSnapshotStore: Save: %w", err)
	}
	if err := s.db.Sync(); err != nil {
		return fmt.Errorf("badgerSnapshotStore: Save sync: %w", err)
	}
	return nil
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
