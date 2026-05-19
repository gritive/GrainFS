package policystore

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrPolicyNotFound = errors.New("policy not found")
	ErrBuiltinPolicy  = errors.New("built-in policy cannot be mutated")
	ErrInUse          = errors.New("policy still attached; detach before delete")
)

type entry struct {
	doc     []byte
	builtin bool
}

// InMemoryStore is a concurrency-safe in-memory store for IAM policy documents.
// Each entry is keyed by policy name and carries the raw JSON bytes plus a
// builtin flag. Built-in policies are immutable: neither Put (from the custom-policy
// path) nor Delete will succeed once a built-in entry is installed.
type InMemoryStore struct {
	mu sync.RWMutex
	m  map[string]entry
}

// NewInMemoryStore returns an empty policy store.
func NewInMemoryStore() *InMemoryStore { return &InMemoryStore{m: make(map[string]entry)} }

// Put stores the (name → doc, builtin) entry. If an existing entry is builtin and
// the caller is not also asserting builtin=true, returns ErrBuiltinPolicy (operator
// cannot overwrite a built-in via the custom-policy path).
func (s *InMemoryStore) Put(_ context.Context, name string, doc []byte, builtin bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.m[name]; ok && existing.builtin && !builtin {
		return ErrBuiltinPolicy
	}
	s.m[name] = entry{doc: append([]byte(nil), doc...), builtin: builtin}
	return nil
}

// GetRaw returns a copy of the policy document bytes. Returns ErrPolicyNotFound if
// the name is not present. The returned bytes are independent of internal storage.
func (s *InMemoryStore) GetRaw(_ context.Context, name string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.m[name]
	if !ok {
		return nil, ErrPolicyNotFound
	}
	return append([]byte(nil), e.doc...), nil
}

// IsBuiltin reports whether the named policy is marked as a built-in.
func (s *InMemoryStore) IsBuiltin(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.m[name].builtin
}

// Delete removes the named policy. Returns ErrPolicyNotFound if absent or
// ErrBuiltinPolicy if the entry is a built-in.
func (s *InMemoryStore) Delete(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[name]
	if !ok {
		return ErrPolicyNotFound
	}
	if e.builtin {
		return ErrBuiltinPolicy
	}
	delete(s.m, name)
	return nil
}

// List returns the names of all stored policies in arbitrary order.
func (s *InMemoryStore) List() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.m))
	for k := range s.m {
		out = append(out, k)
	}
	return out
}
