package chunkref

import "time"

// RefTable is the in-memory, idempotent chunk reference cache.
//
// RefTable is NOT internally synchronized. All mutation happens under a
// serialized apply context (single: one Badger txn; cluster: Raft FSM apply),
// so no internal locking is needed (lock-free by serialization, not by mutex).
// Do not share a RefTable across goroutines without external synchronization.
type RefTable struct {
	refs       map[ChunkID]map[ManifestID]struct{}
	tombstones map[ChunkID]time.Time // chunkID -> t_zero; present only while refcount == 0
}

// NewRefTable returns an empty RefTable.
func NewRefTable() *RefTable {
	return &RefTable{
		refs:       make(map[ChunkID]map[ManifestID]struct{}),
		tombstones: make(map[ChunkID]time.Time),
	}
}

// AddRef idempotently records that manifest m references chunk c. Adding an
// existing (m, c) pair is a no-op (set semantics).
//
// AddRef updates only this cache. The caller is responsible for committing the
// owning manifest descriptor AFTER the ref is recorded (ref-add → descriptor
// commit ordering, per the spec crash-recovery rules); chunkref does not
// enforce that ordering.
func (t *RefTable) AddRef(m ManifestID, c ChunkID) {
	set := t.refs[c]
	if set == nil {
		set = make(map[ManifestID]struct{})
		t.refs[c] = set
	}
	set[m] = struct{}{}
	delete(t.tombstones, c) // referenced again → no longer a GC candidate
}

// RefCount returns the number of distinct manifests referencing c (0 if none).
func (t *RefTable) RefCount(c ChunkID) int {
	return len(t.refs[c])
}

// Has reports whether manifest m currently references chunk c.
func (t *RefTable) Has(m ManifestID, c ChunkID) bool {
	_, ok := t.refs[c][m]
	return ok
}
