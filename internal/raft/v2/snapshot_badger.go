package raftv2

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
//	[Version: 1B = 0x01]
//	[LastIncludedIndex: 8B BE]
//	[LastIncludedTerm:  8B BE]
//	[NumVoters: 4B BE]
//	  Repeated NumVoters times:
//	    [VoterIDLen: 4B BE]
//	    [VoterID: VoterIDLen bytes]
//	[DataLen: 8B BE]
//	[Data: DataLen bytes]
//
// The version prefix exists for forward-compat: future encodings can bump
// it and reject unknown versions on load. Total fixed overhead: 1 + 8 + 8 + 4
// + 8 = 29 bytes plus per-voter and Data lengths.
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

const snapshotEncodingVersion byte = 0x01

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
	val := encodeSnapshot(snap)
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
	// Pre-compute size to allocate exactly once.
	size := 1 + 8 + 8 + 4 // version + lastIdx + lastTerm + numVoters
	for _, v := range snap.Configuration {
		size += 4 + len(v)
	}
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
	binary.BigEndian.PutUint64(buf[off:], uint64(len(snap.Data)))
	off += 8
	copy(buf[off:], snap.Data)
	return buf
}

// decodeSnapshot is the inverse of encodeSnapshot. Errors on any structural
// problem (short buffer, unknown version) so the caller refuses to start
// from a corrupt snapshot.
func decodeSnapshot(val []byte) (*Snapshot, error) {
	if len(val) < 1 {
		return nil, fmt.Errorf("badgerSnapshotStore: empty value")
	}
	if val[0] != snapshotEncodingVersion {
		return nil, fmt.Errorf("badgerSnapshotStore: unknown version 0x%02x", val[0])
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
	return &Snapshot{
		LastIncludedIndex: lastIdx,
		LastIncludedTerm:  lastTerm,
		Configuration:     voters,
		Data:              data,
	}, nil
}
