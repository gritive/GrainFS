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
	// Returns ErrLogIndexOutOfRange if startIdx > LastIndex()+1.
	EntriesFrom(startIdx uint64, maxEntries int) ([]LogEntry, error)
}

// memLogStore is an in-memory LogStore backed by a single slice. It is owned
// exclusively by the actor goroutine and requires no locking. Logical index i
// maps to entries[i-1] (0-based slice, 1-based Raft index).
type memLogStore struct {
	entries []LogEntry
}

func newMemLogStore() *memLogStore { return &memLogStore{} }

func (s *memLogStore) FirstIndex() uint64 { return 1 }

func (s *memLogStore) LastIndex() uint64 {
	if len(s.entries) == 0 {
		return 0
	}
	return uint64(len(s.entries))
}

func (s *memLogStore) Entry(idx uint64) (LogEntry, error) {
	if idx == 0 || idx > uint64(len(s.entries)) {
		return LogEntry{}, ErrLogIndexOutOfRange
	}
	return s.entries[idx-1], nil
}

func (s *memLogStore) TermAt(idx uint64) (uint64, error) {
	if idx == 0 {
		return 0, nil // Raft sentinel: "no previous entry"
	}
	if idx > uint64(len(s.entries)) {
		return 0, ErrLogIndexOutOfRange
	}
	return s.entries[idx-1].Term, nil
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
	if idx >= uint64(len(s.entries)) {
		return nil // no-op
	}
	s.entries = s.entries[:idx]
	return nil
}

func (s *memLogStore) EntriesFrom(startIdx uint64, maxEntries int) ([]LogEntry, error) {
	if startIdx == 0 {
		return nil, ErrLogIndexOutOfRange
	}
	last := uint64(len(s.entries))
	if startIdx > last+1 {
		return nil, ErrLogIndexOutOfRange
	}
	if startIdx > last {
		return nil, nil
	}
	src := s.entries[startIdx-1:]
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
