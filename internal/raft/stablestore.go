package raft

// HardState is the subset of Raft state that must be persisted across restarts
// to satisfy the Raft §5.4.1 safety invariant. Specifically: a node must not
// "forget" who it voted for in a given term, nor regress to an older term.
type HardState struct {
	CurrentTerm uint64
	VotedFor    string // empty string when no vote granted
}

// StableStore persists HardState. All methods are called from the actor
// goroutine; implementations need not be goroutine-safe.
//
// SaveHardState must be durable before returning — the actor relies on
// "if SaveHardState returned, the new term/vote will survive a crash" for
// the safety argument.
type StableStore interface {
	HardState() (HardState, error) // returns zero HardState on first open
	SaveHardState(hs HardState) error
}

type memStableStore struct {
	hs HardState
}

func newMemStableStore() *memStableStore { return &memStableStore{} }

func (s *memStableStore) HardState() (HardState, error) { return s.hs, nil }

func (s *memStableStore) SaveHardState(hs HardState) error {
	s.hs = hs
	return nil
}
