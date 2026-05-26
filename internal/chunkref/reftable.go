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

// RemoveRef idempotently removes manifest m's reference to chunk c. Removing a
// non-existent (m, c) pair is a no-op. When the removal drops c's refcount to 0,
// a tombstone is recorded with t_zero = now, marking when the chunk became
// unreferenced (GCCandidates uses it for the retention window).
func (t *RefTable) RemoveRef(m ManifestID, c ChunkID, now time.Time) {
	set := t.refs[c]
	if set == nil {
		return
	}
	if _, ok := set[m]; !ok {
		return
	}
	delete(set, m)
	if len(set) == 0 {
		delete(t.refs, c)
		t.tombstones[c] = now // t_zero
	}
}

// GCCandidate is a chunk eligible for garbage collection by the cache's local
// conditions, paired with the t_zero at which it became unreferenced. The caller
// MUST re-check, immediately before deleting, that the chunk's current tombstone
// still carries this same TZero: a chunk can be re-referenced and unreferenced
// again between the scan and the delete, resetting its retention window. Deleting
// on a stale TZero would delete a chunk whose new window has not yet elapsed
// (PITR data loss).
type GCCandidate struct {
	ChunkID ChunkID
	TZero   time.Time
}

// GCCandidates returns chunks meeting the cache's two LOCAL garbage-collection
// conditions: refcount == 0 AND (now - t_zero) > window. Each candidate carries
// its t_zero so the caller can detect re-reference churn (see GCCandidate).
//
// Tombstoned chunks are the once-referenced, currently-unreferenced chunks
// (refcount == 0); a chunk that was never referenced has refcount 0 but no
// tombstone and never appears here. The tombstone set is therefore NOT the sole
// GC source: chunks unreferenced before the last Rebuild are invisible to this
// method, and the authoritative orphan walk (spec OrphanSegmentWalkable) is what
// catches those. Use this set for retention-window timing of newly-unreferenced
// chunks; iteration then stays proportional to that set (incremental-friendly).
//
// This is NOT sufficient for physical deletion. The caller (scrubber) MUST
// re-verify the third condition — absence from the authoritative manifest set —
// AND the TZero generation (see GCCandidate) immediately before deleting, because
// this derived cache may be stale. See spec
// "GC = 보존 윈도우 밖 AND cache ref==0 AND authoritative manifest absence".
//
// The returned slice order is unspecified (map iteration).
func (t *RefTable) GCCandidates(now time.Time, window time.Duration) []GCCandidate {
	var out []GCCandidate
	for c, tZero := range t.tombstones {
		if now.Sub(tZero) > window {
			out = append(out, GCCandidate{ChunkID: c, TZero: tZero})
		}
	}
	return out
}
