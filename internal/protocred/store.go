package protocred

import (
	"crypto/sha256"
	"sort"
	"sync"
	"time"
)

type Store struct {
	mu    sync.Mutex
	items map[string]Credential
}

func NewStore() *Store {
	return &Store{items: make(map[string]Credential)}
}

func (s *Store) put(item Credential) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[item.ID] = item
}

func (s *Store) get(id string) (Credential, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[id]
	return item, ok
}

func (s *Store) list(filter ListFilter) []Credential {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Credential, 0, len(s.items))
	for _, item := range s.items {
		if filter.SAID != "" && item.SAID != filter.SAID {
			continue
		}
		if filter.Protocol != "" && item.Protocol != filter.Protocol {
			continue
		}
		out = append(out, item)
	}
	return out
}

func (s *Store) update(id string, fn func(Credential) Credential) (Credential, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[id]
	if !ok {
		return Credential{}, false
	}
	item = fn(item)
	s.items[id] = item
	return item, true
}

// Snapshot returns a deterministic, detached copy of all credential rows.
func (s *Store) Snapshot() []Credential {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]Credential, 0, len(s.items))
	for _, item := range s.items {
		out = append(out, cloneCredential(item))
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})
	return out
}

// Restore replaces the store contents with detached copies of rows.
func (s *Store) Restore(rows []Credential) {
	s.mu.Lock()
	defer s.mu.Unlock()

	items := make(map[string]Credential, len(rows))
	for _, row := range rows {
		items[row.ID] = cloneCredential(row)
	}
	s.items = items
}

// ApplyCreate inserts a fully-materialized credential row from durable FSM
// state. Replaying the same row is a success; conflicting rows fail loud.
func (s *Store) ApplyCreate(row Credential) (Credential, error) {
	if row.ID == "" || row.SAID == "" || row.Resource == "" || !validProtocol(row.Protocol) || !validMode(row.Mode) {
		return Credential{}, ErrInvalid
	}
	if row.Generation == 0 {
		row.Generation = 1
	}
	row = cloneCredential(row)

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.items[row.ID]; ok {
		if credentialsEqual(existing, row) {
			return cloneCredential(existing), nil
		}
		return Credential{}, ErrConflict
	}
	s.items[row.ID] = row
	return cloneCredential(row), nil
}

// ApplyRotate replaces secret material for an existing, non-revoked row.
func (s *Store) ApplyRotate(id string, hash [sha256.Size]byte, hint string) (Credential, error) {
	return s.ApplyRotateWithSecretEnc(id, hash, hint, nil)
}

func (s *Store) ApplyRotateWithSecretEnc(id string, hash [sha256.Size]byte, hint string, enc []byte) (Credential, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[id]
	if !ok {
		return Credential{}, ErrNotFound
	}
	if item.RevokedAt != nil {
		return Credential{}, ErrRevoked
	}
	item.SecretHash = hash
	item.SecretHint = hint
	item.SecretEnc = cloneBytes(enc)
	item.Generation++
	s.items[id] = item
	return cloneCredential(item), nil
}

// ApplyRevoke marks a credential revoked once. Replays preserve the first
// revocation timestamp.
func (s *Store) ApplyRevoke(id string, revokedAt time.Time) (Credential, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[id]
	if !ok {
		return Credential{}, ErrNotFound
	}
	if item.RevokedAt == nil {
		item.RevokedAt = cloneTime(&revokedAt)
		item.Generation++
		s.items[id] = item
	}
	return cloneCredential(item), nil
}

// ApplyMarkStale records the first stale marker for a credential.
func (s *Store) ApplyMarkStale(id string, staleAt time.Time, reason string) (Credential, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[id]
	if !ok {
		return Credential{}, ErrNotFound
	}
	if item.StaleAt == nil {
		item.StaleAt = cloneTime(&staleAt)
		item.StaleReason = reason
		item.Generation++
		s.items[id] = item
	}
	return cloneCredential(item), nil
}

// ApplyLastUsed records the newest observed use timestamp.
func (s *Store) ApplyLastUsed(id string, usedAt time.Time) (Credential, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[id]
	if !ok {
		return Credential{}, ErrNotFound
	}
	if item.LastUsedAt == nil || item.LastUsedAt.Before(usedAt) {
		item.LastUsedAt = cloneTime(&usedAt)
		item.Generation++
		s.items[id] = item
	}
	return cloneCredential(item), nil
}

func credentialsEqual(a, b Credential) bool {
	return a.ID == b.ID &&
		a.SAID == b.SAID &&
		a.Protocol == b.Protocol &&
		a.Resource == b.Resource &&
		a.Mode == b.Mode &&
		a.SecretHash == b.SecretHash &&
		a.SecretHint == b.SecretHint &&
		string(a.SecretEnc) == string(b.SecretEnc) &&
		a.CreatedAt.Equal(b.CreatedAt) &&
		a.CreatedBy == b.CreatedBy &&
		timePtrEqual(a.ExpiresAt, b.ExpiresAt) &&
		timePtrEqual(a.RevokedAt, b.RevokedAt) &&
		timePtrEqual(a.LastUsedAt, b.LastUsedAt) &&
		a.Generation == b.Generation &&
		timePtrEqual(a.StaleAt, b.StaleAt) &&
		a.StaleReason == b.StaleReason
}

func timePtrEqual(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}
