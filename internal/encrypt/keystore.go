package encrypt

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// ErrKEKVersionUnknown is returned by Get/Delete when the requested version
// is not present in the store.
var ErrKEKVersionUnknown = errors.New("KEK version unknown")

// ErrKEKVersionDuplicate is returned by Add when the version is already
// present. Adding the same version twice is a programmer error — the caller
// should Get-then-update if intentional replacement is required.
var ErrKEKVersionDuplicate = errors.New("KEK version already present")

// ErrKEKActiveInUse is returned by Delete when the caller attempts to remove
// the active version. The active version must be advanced via Add(higher)
// before the old version can be deleted.
var ErrKEKActiveInUse = errors.New("cannot delete active KEK version")

// KEKStore holds multiple KEK versions in memory. The highest-numbered
// version is the active one (the one used to wrap freshly-generated DEKs).
//
// Concurrency: a sync.RWMutex guards the keys map. KEK operations (Add,
// Delete) are admin-rate, not hot-path — a Mutex would suffice, but Get
// runs on the hot path during DEK operations, hence RWMutex.
type KEKStore struct {
	mu     sync.RWMutex
	keys   map[uint32][]byte
	active uint32
}

// NewKEKStore creates an empty KEKStore.
func NewKEKStore() *KEKStore {
	return &KEKStore{keys: make(map[uint32][]byte)}
}

// Add inserts kek at version. Refuses len(kek) != KEKSize and duplicate
// versions. Advancing the active version is a side effect when version >
// current active.
func (s *KEKStore) Add(version uint32, kek []byte) error {
	if len(kek) != KEKSize {
		return fmt.Errorf("KEKStore.Add: kek len = %d, want %d", len(kek), KEKSize)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.keys[version]; dup {
		return ErrKEKVersionDuplicate
	}
	stored := append([]byte(nil), kek...)
	s.keys[version] = stored
	if len(s.keys) == 1 || version > s.active {
		s.active = version
	}
	return nil
}

// Get returns a copy of the KEK at version. Copy prevents callers from
// mutating internal state.
func (s *KEKStore) Get(version uint32) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	k, ok := s.keys[version]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrKEKVersionUnknown, version)
	}
	out := make([]byte, KEKSize)
	copy(out, k)
	return out, nil
}

// ActiveVersion returns the current active KEK version. Returns 0 on an
// empty store (caller must check Versions() if 0-vs-empty matters).
func (s *KEKStore) ActiveVersion() uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active
}

// ActiveKEK returns the KEK bytes for the active version. Convenience over
// Get(ActiveVersion()).
func (s *KEKStore) ActiveKEK() ([]byte, error) {
	s.mu.RLock()
	v := s.active
	s.mu.RUnlock()
	return s.Get(v)
}

// Versions returns all stored versions in ascending order. Returns a fresh
// slice — caller may mutate.
func (s *KEKStore) Versions() []uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]uint32, 0, len(s.keys))
	for v := range s.keys {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Delete removes the KEK at version. Refuses deletion of the active
// version (must advance active first). Refuses unknown versions. Zeroizes
// in-memory bytes before unlinking from the map.
func (s *KEKStore) Delete(version uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if version == s.active {
		return ErrKEKActiveInUse
	}
	bytes, ok := s.keys[version]
	if !ok {
		return fmt.Errorf("%w: %d", ErrKEKVersionUnknown, version)
	}
	for i := range bytes {
		bytes[i] = 0
	}
	delete(s.keys, version)
	return nil
}
