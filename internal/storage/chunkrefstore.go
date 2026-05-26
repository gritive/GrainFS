package storage

import (
	"encoding/binary"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/chunkref"
)

const (
	refMembershipPrefix = "refc:"
	refTombstonePrefix  = "refts:"
	refKeySep           = "\x01" // ChunkID (locator string) never contains \x01
)

// ChunkRefStore mirrors chunkref.RefTable's idempotent (manifestID, chunkID) set
// semantics onto a single-node BadgerDB transaction, so refcount mutations commit
// atomically with the object-manifest write (spec single-mode consistency model).
// It is a thin txn-scoped view; construct one per operation around the caller's
// *badger.Txn. refcount remains a DERIVED cache — the manifest set is truth and
// the store is rebuildable.
type ChunkRefStore struct {
	txn *badger.Txn
}

func refMembershipChunkPrefix(c chunkref.ChunkID) []byte {
	return []byte(refMembershipPrefix + string(c) + refKeySep)
}

func refMembershipKey(m chunkref.ManifestID, c chunkref.ChunkID) []byte {
	key := refMembershipChunkPrefix(c)
	key = append(key, byte(m.Domain))
	key = append(key, m.VersionID...)
	return key
}

func refTombstoneKey(c chunkref.ChunkID) []byte {
	return []byte(refTombstonePrefix + string(c))
}

// AddRef idempotently records manifest m's reference to chunk c and evicts any
// tombstone (c is referenced again, so no longer a GC candidate).
func (s ChunkRefStore) AddRef(m chunkref.ManifestID, c chunkref.ChunkID) error {
	if err := s.txn.Set(refMembershipKey(m, c), nil); err != nil {
		return err
	}
	return s.deleteTombstone(c)
}

// RemoveRef idempotently removes manifest m's reference to c. When the removal
// drops refcount to 0, a tombstone is written with t_zero = now.
func (s ChunkRefStore) RemoveRef(m chunkref.ManifestID, c chunkref.ChunkID, now time.Time) error {
	mk := refMembershipKey(m, c)
	if _, err := s.txn.Get(mk); err == badger.ErrKeyNotFound {
		return nil
	} else if err != nil {
		return err
	}
	if err := s.txn.Delete(mk); err != nil {
		return err
	}
	n, err := s.RefCount(c)
	if err != nil {
		return err
	}
	if n == 0 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(now.UnixNano()))
		return s.txn.Set(refTombstoneKey(c), buf[:])
	}
	return nil
}

// RefCount returns the number of distinct manifests referencing c.
func (s ChunkRefStore) RefCount(c chunkref.ChunkID) (int, error) {
	prefix := refMembershipChunkPrefix(c)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Prefix = prefix
	it := s.txn.NewIterator(opts)
	defer it.Close()
	n := 0
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		n++
	}
	return n, nil
}

// TombstoneTime returns the recorded t_zero for c (when it became unreferenced),
// and whether a tombstone is present.
func (s ChunkRefStore) TombstoneTime(c chunkref.ChunkID) (time.Time, bool, error) {
	item, err := s.txn.Get(refTombstoneKey(c))
	if err == badger.ErrKeyNotFound {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	var nanos uint64
	if err := item.Value(func(v []byte) error {
		nanos = binary.BigEndian.Uint64(v)
		return nil
	}); err != nil {
		return time.Time{}, false, err
	}
	return time.Unix(0, int64(nanos)), true, nil
}

func (s ChunkRefStore) deleteTombstone(c chunkref.ChunkID) error {
	if err := s.txn.Delete(refTombstoneKey(c)); err != nil && err != badger.ErrKeyNotFound {
		return err
	}
	return nil
}
