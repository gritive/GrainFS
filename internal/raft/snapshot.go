package raft

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
	Index             uint64
	Term              uint64
	Servers           []Server
	// Configuration at snapshot time. Voter ID list only — learners ride
	// in Learners (added M6.0). Empty/nil for tests that don't care about
	// config recovery.
	Configuration []string
	// Learners are non-voting observers at snapshot time. id → address.
	// Nil/empty when no learners. Added M6.0 (Path B); pre-M6.0 snapshots
	// decode with Learners == nil and the schema gate refuses the store.
	Learners map[string]string
	// Data is opaque FSM state bytes provided by the caller of CreateSnapshot.
	Data []byte
}

type SnapshotStatus struct {
	Available bool
	Index     uint64
	Term      uint64
	SizeBytes int
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
	if len(snap.Learners) > 0 {
		cp.Learners = make(map[string]string, len(snap.Learners))
		for k, v := range snap.Learners {
			cp.Learners[k] = v
		}
	}
	if len(snap.Data) > 0 {
		cp.Data = make([]byte, len(snap.Data))
		copy(cp.Data, snap.Data)
	}
	s.snap = &cp
	return nil
}
