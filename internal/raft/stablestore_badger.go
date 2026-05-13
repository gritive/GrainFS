package raft

import (
	"encoding/binary"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

// badgerStableStore persists HardState in a BadgerDB instance under a caller-
// supplied prefix. A single key (prefix + "current") holds the encoded state.
//
// Encoding: 8 bytes BE (CurrentTerm) || 4 bytes BE (VotedForLen) || VotedFor.
// NOT JSON. Total: 12 + len(VotedFor) bytes.
//
// On first open (key absent) HardState() returns a zero HardState, equivalent
// to the fresh-start case (CurrentTerm=0, VotedFor="").
//
// Durability: SaveHardState calls db.Sync() after the write transaction
// commits, so the HardState is power-loss durable on return regardless of
// whether the caller opened the DB with SyncWrites=true or false. This is
// required for Raft §5.4.1 — a node MUST NOT forget its vote across reboots.
// Caller can still pass SyncWrites=true to make every Badger write durable
// (recommended for production); the explicit Sync() here makes the StableStore
// safe even when the DB is shared with other components that do not need
// per-write durability.
type badgerStableStore struct {
	db        *badger.DB
	keyPrefix []byte // copied at construction; immutable
}

// newBadgerStableStore opens a StableStore view into db under the given prefix.
// Multiple Raft groups can coexist in one Badger DB with distinct prefixes.
func newBadgerStableStore(db *badger.DB, prefix []byte) (*badgerStableStore, error) {
	p := make([]byte, len(prefix))
	copy(p, prefix)
	return &badgerStableStore{db: db, keyPrefix: p}, nil
}

// currentKey returns the single key for the HardState record.
func (s *badgerStableStore) currentKey() []byte {
	const suffix = "current"
	key := make([]byte, len(s.keyPrefix)+len(suffix))
	copy(key, s.keyPrefix)
	copy(key[len(s.keyPrefix):], suffix)
	return key
}

// HardState reads the persisted HardState. Returns a zero HardState on first
// open (key not found). Any other error is wrapped and returned.
func (s *badgerStableStore) HardState() (HardState, error) {
	var hs HardState
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(s.currentKey())
		if err == badger.ErrKeyNotFound {
			return nil // first run — zero HardState
		}
		if err != nil {
			return fmt.Errorf("badgerStableStore: HardState: %w", err)
		}
		return item.Value(func(val []byte) error {
			var decErr error
			hs, decErr = decodeHardState(val)
			return decErr
		})
	})
	if err != nil {
		return HardState{}, err
	}
	return hs, nil
}

// SaveHardState persists hs. The write is power-loss durable on return:
// after the Badger Update transaction commits, db.Sync() flushes the value
// log to disk so the HardState survives both process crashes and OS/hardware
// crashes. This explicit sync is required because badger.DefaultOptions sets
// SyncWrites=false; relying on the default would leave the vote/term in
// page cache on commit, violating Raft §5.4.1 across hard reboots.
func (s *badgerStableStore) SaveHardState(hs HardState) error {
	val := encodeHardState(hs)
	err := s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(s.currentKey(), val); err != nil {
			return fmt.Errorf("badgerStableStore: SaveHardState: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("badgerStableStore: SaveHardState: %w", err)
	}
	if err := s.db.Sync(); err != nil {
		return fmt.Errorf("badgerStableStore: SaveHardState sync: %w", err)
	}
	return nil
}

// encodeHardState serialises HardState into binary format:
// [CurrentTerm: 8 bytes BE][VotedForLen: 4 bytes BE][VotedFor: VotedForLen bytes]
func encodeHardState(hs HardState) []byte {
	votedForBytes := []byte(hs.VotedFor)
	buf := make([]byte, 8+4+len(votedForBytes))
	binary.BigEndian.PutUint64(buf[0:], hs.CurrentTerm)
	binary.BigEndian.PutUint32(buf[8:], uint32(len(votedForBytes)))
	copy(buf[12:], votedForBytes)
	return buf
}

// decodeHardState deserialises HardState from the binary format.
func decodeHardState(val []byte) (HardState, error) {
	if len(val) < 12 {
		return HardState{}, fmt.Errorf("badgerStableStore: short HardState value (%d bytes)", len(val))
	}
	term := binary.BigEndian.Uint64(val[0:])
	vfLen := int(binary.BigEndian.Uint32(val[8:]))
	if len(val) < 12+vfLen {
		return HardState{}, fmt.Errorf("badgerStableStore: HardState value too short for VotedFor (%d < %d)", len(val), 12+vfLen)
	}
	votedFor := ""
	if vfLen > 0 {
		votedFor = string(val[12 : 12+vfLen])
	}
	return HardState{CurrentTerm: term, VotedFor: votedFor}, nil
}
