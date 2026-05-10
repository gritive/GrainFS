package raftv2

// Snapshot is the immutable state snapshot at a particular log index. It
// captures the FSM state (caller-provided opaque bytes) plus the metadata
// needed to validate a peer's progress (last included index/term + cluster
// configuration at the time of snapshot).
//
// Raft §7: a snapshot represents the system state through LastIncludedIndex.
// After a snapshot is durable, log entries up to and including LastIncludedIndex
// can be discarded (CompactBefore on the LogStore).
type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	// Configuration at snapshot time. Just the voter ID list for now;
	// joint-config encoding is task 2's concern. Empty/nil for tests
	// that don't care about config recovery.
	Configuration []string
	// Data is opaque FSM state bytes provided by the caller of CreateSnapshot.
	Data []byte
}

// SnapshotStore manages the durable snapshot. PR 15 keeps "at most one
// snapshot at a time" — Save replaces the prior. Multi-snapshot retention
// (for incremental sends, etc.) is out of scope. Single-goroutine access
// from the actor; impls do not need to be thread-safe.
type SnapshotStore interface {
	// Latest returns the most recent snapshot, or (nil, nil) if none.
	Latest() (*Snapshot, error)
	// Save persists snap durably; returns once on disk (post-fsync).
	// Replaces any prior snapshot atomically.
	Save(snap *Snapshot) error
}

// memSnapshotStore is an in-memory SnapshotStore for tests / non-persistent
// nodes. Single-goroutine access from the actor — no locking required.
type memSnapshotStore struct {
	snap *Snapshot
}

func newMemSnapshotStore() *memSnapshotStore { return &memSnapshotStore{} }

func (s *memSnapshotStore) Latest() (*Snapshot, error) { return s.snap, nil }

func (s *memSnapshotStore) Save(snap *Snapshot) error {
	// Defensive copy of the Configuration and Data slices so a caller mutating
	// them after Save cannot corrupt the stored snapshot.
	if snap == nil {
		s.snap = nil
		return nil
	}
	cp := *snap
	if len(snap.Configuration) > 0 {
		cp.Configuration = make([]string, len(snap.Configuration))
		copy(cp.Configuration, snap.Configuration)
	}
	if len(snap.Data) > 0 {
		cp.Data = make([]byte, len(snap.Data))
		copy(cp.Data, snap.Data)
	}
	s.snap = &cp
	return nil
}
