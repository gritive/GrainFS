package cluster

import (
	"sync"
	"time"

	"github.com/gritive/GrainFS/internal/iam/jwt"
)

// JWTKeyStore holds the wrapped JWT signing keys persisted by the meta-FSM.
// At most one current and one previous key. Thread-safe.
type JWTKeyStore struct {
	mu       sync.RWMutex
	current  *jwt.KeySeed
	previous *jwt.KeySeed
}

// NewJWTKeyStore returns an empty JWTKeyStore.
func NewJWTKeyStore() *JWTKeyStore { return &JWTKeyStore{} }

// Put stores seed, routing by seed.Role ("current" or "previous").
// Roles other than "current"/"previous" are silently ignored.
func (s *JWTKeyStore) Put(seed jwt.KeySeed) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := seed
	switch seed.Role {
	case "current":
		s.current = &cp
	case "previous":
		s.previous = &cp
	}
}

// Demote moves current → previous, stamping DemotedAt = now.Unix().
// No-op if current is nil.
func (s *JWTKeyStore) Demote(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil {
		return
	}
	cp := *s.current
	cp.Role = "previous"
	cp.DemotedAt = now.Unix()
	s.previous = &cp
	s.current = nil
}

// Snapshot returns clones of current and previous (either or both may be nil).
func (s *JWTKeyStore) Snapshot() (current, previous *jwt.KeySeed) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.current != nil {
		cp := *s.current
		current = &cp
	}
	if s.previous != nil {
		cp := *s.previous
		previous = &cp
	}
	return current, previous
}

// ReplaceAll atomically replaces both slots. Nil pointers clear the slot.
// Used during Restore.
func (s *JWTKeyStore) ReplaceAll(current, previous *jwt.KeySeed) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current != nil {
		cp := *current
		s.current = &cp
	} else {
		s.current = nil
	}
	if previous != nil {
		cp := *previous
		s.previous = &cp
	} else {
		s.previous = nil
	}
}

// PrunePrevSafe returns true when the previous key can be safely removed:
// its DemotedAt + MaxJWTTokenTTL is before now, meaning all tokens it could
// have signed have expired.
func (s *JWTKeyStore) PrunePrevSafe(now time.Time) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.previous == nil {
		return true // nothing to prune
	}
	if s.previous.DemotedAt == 0 {
		return false // no demotion timestamp recorded; be conservative
	}
	expiresAt := time.Unix(s.previous.DemotedAt, 0).Add(jwt.MaxJWTTokenTTL)
	return now.After(expiresAt)
}

// RemovePrev clears the previous key slot.
func (s *JWTKeyStore) RemovePrev() {
	s.mu.Lock()
	s.previous = nil
	s.mu.Unlock()
}
