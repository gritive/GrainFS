package raftv2

import "errors"

// ErrLogIndexOutOfRange is returned by LogStore methods when the requested
// index is outside the valid range (0 or > LastIndex()). Log indexing bugs
// are programmer errors, not recoverable runtime conditions; callers in the
// actor goroutine are expected to panic on this error (see mustEntry /
// mustTermAt in actor.go).
var ErrLogIndexOutOfRange = errors.New("raftv2: log index out of range")

// LogStore manages the Raft log. All methods are called exclusively from the
// actor goroutine; implementations need not be goroutine-safe.
//
// Index convention: 1-based per the Raft paper. Index 0 is the sentinel
// "no previous entry" (TermAt(0) returns 0 by convention; Entry(0) errors).
//
// Error semantics: out-of-range accesses return ErrLogIndexOutOfRange and are
// treated as programmer bugs. The actor panics on such errors. Persistent
// implementations may additionally return I/O errors; those propagate up and
// should also cause a panic (log corruption is not recoverable without a
// crash-recovery procedure handled by PR 11+).
//
// Canonical Command form: callers and implementations should treat
// `len(Command) == 0` as equivalent to `Command == nil`. Persistent
// implementations are free to round-trip an empty []byte as nil
// (badgerLogStore does so). Application FSMs MUST length-check rather than
// nil-check Command, especially for LogEntryNoOp (Command is always nil for
// no-ops).
type LogStore interface {
	// FirstIndex returns the index of the first entry in the log.
	// Returns 1 when the log is empty (next append goes to index 1).
	FirstIndex() uint64

	// LastIndex returns the index of the last entry in the log.
	// Returns 0 when the log is empty.
	LastIndex() uint64

	// Entry returns the log entry at the given 1-based logical index.
	// Returns ErrLogIndexOutOfRange if idx == 0 or idx > LastIndex().
	Entry(idx uint64) (LogEntry, error)

	// TermAt returns the term of the entry at 1-based logical index idx.
	// Returns (0, nil) when idx == 0 (Raft sentinel for "no previous entry").
	// Returns ErrLogIndexOutOfRange if idx > LastIndex().
	TermAt(idx uint64) (uint64, error)

	// Append appends entries to the log. Each entry's Index must equal
	// LastIndex()+1, +2, … in order. Caller is responsible for ensuring
	// contiguity. Implementations validate the leading entry's Index against
	// LastIndex()+1 and panic on mismatch; intra-batch contiguity is NOT
	// validated, so a caller passing [{Index:N}, {Index:N+2}] silently
	// corrupts the log. All current callers pass naturally-contiguous slices.
	Append(entries []LogEntry) error

	// TruncateAfter removes all entries with index > idx. After this call,
	// LastIndex() == idx. No-op if idx >= LastIndex() — nothing to remove
	// because the store is already at or shorter than the requested suffix
	// boundary. Persistent implementations must durably remove the truncated
	// suffix before returning (BadgerDB backing lands in PR 10).
	TruncateAfter(idx uint64) error

	// EntriesFrom returns entries starting at startIdx through LastIndex(),
	// capped at maxEntries. The returned slice is a copy — it does NOT alias
	// internal storage — so the caller may retain it across subsequent
	// mutations of the store (e.g. across a step-down truncation).
	// Returns ErrLogIndexOutOfRange if startIdx > LastIndex()+1, OR if
	// startIdx < FirstIndex() (caller cannot ask for entries already
	// compacted into a snapshot).
	EntriesFrom(startIdx uint64, maxEntries int) ([]LogEntry, error)

	// CompactBefore removes all entries with index <= boundary. After a
	// successful call, FirstIndex() returns boundary+1 (or LastIndex()+1 if
	// the log is now empty). Implementations must persist the new boundary
	// durably before returning.
	//
	// CompactBefore is the only operation that advances FirstIndex past 1.
	// Callers MUST ensure boundary <= the snapshot's LastIncludedIndex; the
	// snapshot itself covers all entries up to and including boundary.
	//
	// The store also tracks the term of the entry at boundary (the snapshot's
	// LastIncludedTerm) so TermAt(boundary) continues to return the correct
	// term after compaction. This is load-bearing for AE consistency: when
	// the leader sends entries starting at boundary+1, PrevLogTerm equals
	// the snapshot's LastIncludedTerm. Queries at indices below boundary
	// return ErrLogIndexOutOfRange (compacted away); idx == 0 still returns 0
	// (Raft sentinel).
	//
	// No-op if boundary < FirstIndex() (already compacted past). Must error
	// if boundary > LastIndex() — caller bug, no entry covers that index.
	CompactBefore(boundary uint64) error
}

// memLogStore is an in-memory LogStore backed by a single slice. It is owned
// exclusively by the actor goroutine and requires no locking.
//
// Index translation: logical Raft index i maps to entries[i-firstIndex]. Before
// any CompactBefore call, firstIndex == 1, so the mapping reduces to
// entries[i-1]. After CompactBefore(N), firstIndex = N+1 and prevTerm holds
// the term of the (now-discarded) entry at index N — TermAt(N) returns
// prevTerm so AE consistency checks at the snapshot boundary still work.
type memLogStore struct {
	entries    []LogEntry
	firstIndex uint64 // 1-based; 1 when no compaction
	// prevTerm is the term of the entry at firstIndex-1 (== snapshot's
	// LastIncludedTerm after CompactBefore). 0 before any compaction (no
	// snapshot exists). TermAt(firstIndex-1) returns this value so the
	// leader's PrevLogTerm at the snapshot boundary still resolves.
	prevTerm uint64
}

func newMemLogStore() *memLogStore { return &memLogStore{firstIndex: 1} }

func (s *memLogStore) FirstIndex() uint64 {
	if s.firstIndex == 0 {
		return 1
	}
	return s.firstIndex
}

func (s *memLogStore) LastIndex() uint64 {
	if len(s.entries) == 0 {
		// firstIndex-1 is the snapshot boundary (or 0 when no compaction);
		// LastIndex must equal firstIndex-1 for an empty post-compaction log
		// so subsequent appends land at firstIndex (= LastIndex()+1).
		return s.FirstIndex() - 1
	}
	return s.FirstIndex() + uint64(len(s.entries)) - 1
}

func (s *memLogStore) Entry(idx uint64) (LogEntry, error) {
	first := s.FirstIndex()
	if idx == 0 || idx < first || idx > s.LastIndex() {
		return LogEntry{}, ErrLogIndexOutOfRange
	}
	return s.entries[idx-first], nil
}

func (s *memLogStore) TermAt(idx uint64) (uint64, error) {
	if idx == 0 {
		return 0, nil // Raft sentinel: "no previous entry"
	}
	first := s.FirstIndex()
	// Snapshot-boundary fast path: TermAt(firstIndex-1) returns prevTerm so
	// the leader's PrevLogTerm at the boundary still resolves after compaction.
	// Without compaction, first==1 → boundary==0 (already handled above).
	if idx == first-1 {
		return s.prevTerm, nil
	}
	if idx < first || idx > s.LastIndex() {
		return 0, ErrLogIndexOutOfRange
	}
	return s.entries[idx-first].Term, nil
}

func (s *memLogStore) Append(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	if entries[0].Index != s.LastIndex()+1 {
		panic("raftv2: Append: non-contiguous index")
	}
	s.entries = append(s.entries, entries...)
	return nil
}

func (s *memLogStore) TruncateAfter(idx uint64) error {
	last := s.LastIndex()
	if idx >= last {
		return nil // no-op
	}
	first := s.FirstIndex()
	// idx < first-1 would discard the snapshot boundary itself — invalid.
	// idx == first-1 is the "truncate everything since the snapshot" case;
	// leaves the slice empty but firstIndex/prevTerm intact.
	if idx < first-1 {
		// Defensive: mirror Append's panic-on-bug philosophy. Truncating below
		// the snapshot boundary is a programmer error.
		panic("raftv2: TruncateAfter: idx below FirstIndex-1 (snapshot boundary)")
	}
	// keep entries[0 .. idx-first+1) (i.e., logical indices [first, idx]).
	keep := idx - first + 1
	s.entries = s.entries[:keep]
	return nil
}

func (s *memLogStore) EntriesFrom(startIdx uint64, maxEntries int) ([]LogEntry, error) {
	if startIdx == 0 {
		return nil, ErrLogIndexOutOfRange
	}
	first := s.FirstIndex()
	if startIdx < first {
		// Caller asked for entries already compacted into a snapshot. They
		// must use the snapshot path instead.
		return nil, ErrLogIndexOutOfRange
	}
	last := s.LastIndex()
	if startIdx > last+1 {
		return nil, ErrLogIndexOutOfRange
	}
	if startIdx > last {
		return nil, nil
	}
	src := s.entries[startIdx-first:]
	if maxEntries > 0 && len(src) > maxEntries {
		src = src[:maxEntries]
	}
	// Deep-copy: shallow copy(out, src) would leave each entry's Command
	// []byte aliasing internal storage, so a caller mutating Command bytes
	// (or a subsequent Append reusing that backing array) would silently
	// corrupt the in-flight AE payload. PR 6b's note on buildAppendEntriesArgs
	// is the original aliasing concern; this carries it through the LogStore
	// boundary.
	out := make([]LogEntry, len(src))
	copy(out, src)
	for i := range out {
		if len(out[i].Command) > 0 {
			cmd := make([]byte, len(out[i].Command))
			copy(cmd, out[i].Command)
			out[i].Command = cmd
		}
	}
	return out, nil
}

// InstallSnapshotBoundary seeds firstIndex / prevTerm directly without
// requiring entries to exist at the boundary. Used by InstallSnapshot RPC
// when the leader's snapshot covers indices the follower has never seen.
// The log MUST be empty at call time (caller ensures via TruncateAfter(0)).
//
// Not part of the LogStore interface — only InstallSnapshot calls it, and
// it is type-asserted via the unexported snapshotInstaller interface.
func (s *memLogStore) InstallSnapshotBoundary(lastIncludedIndex, lastIncludedTerm uint64) error {
	if len(s.entries) > 0 {
		return ErrLogIndexOutOfRange
	}
	s.firstIndex = lastIncludedIndex + 1
	s.prevTerm = lastIncludedTerm
	return nil
}

// CompactBefore drops entries with index <= boundary. The term of the entry
// at boundary is captured into prevTerm so TermAt(boundary) continues to
// return the snapshot's LastIncludedTerm after the entries are gone.
func (s *memLogStore) CompactBefore(boundary uint64) error {
	first := s.FirstIndex()
	if boundary < first {
		return nil // already compacted past this point
	}
	last := s.LastIndex()
	if boundary > last {
		return ErrLogIndexOutOfRange
	}
	// Capture the term at boundary BEFORE we discard it.
	t, err := s.TermAt(boundary)
	if err != nil {
		return err
	}
	// drop = number of entries to discard = boundary - first + 1.
	drop := boundary - first + 1
	// Reslice: keep entries[drop:]. Zero the dropped slots so any retained
	// Command []byte slices can be GC'd.
	for i := uint64(0); i < drop; i++ {
		s.entries[i] = LogEntry{}
	}
	s.entries = s.entries[drop:]
	// Reset to a fresh slice when empty so the underlying array can be GC'd.
	if len(s.entries) == 0 {
		s.entries = nil
	}
	s.firstIndex = boundary + 1
	s.prevTerm = t
	return nil
}
