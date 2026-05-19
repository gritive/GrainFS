package policyattach

import (
	"context"
	"sync"
)

// InMemoryStore holds SA→policy and group→policy attach mappings.
// All methods are safe for concurrent use.
type InMemoryStore struct {
	mu       sync.RWMutex
	saToPols map[string]map[string]struct{}
	grpToPol map[string]map[string]struct{}
}

// NewInMemoryStore returns an empty InMemoryStore.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		saToPols: make(map[string]map[string]struct{}),
		grpToPol: make(map[string]map[string]struct{}),
	}
}

// AttachToSA attaches policy to the service account identified by saID.
func (s *InMemoryStore) AttachToSA(_ context.Context, saID, policy string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saToPols[saID] == nil {
		s.saToPols[saID] = make(map[string]struct{})
	}
	s.saToPols[saID][policy] = struct{}{}
	return nil
}

// DetachFromSA removes policy from the service account identified by saID.
// Detaching a policy that was never attached is a no-op.
func (s *InMemoryStore) DetachFromSA(_ context.Context, saID, policy string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saToPols[saID] != nil {
		delete(s.saToPols[saID], policy)
	}
	return nil
}

// SAPolicies returns the list of policies directly attached to saID.
func (s *InMemoryStore) SAPolicies(_ context.Context, saID string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.saToPols[saID]))
	for p := range s.saToPols[saID] {
		out = append(out, p)
	}
	return out, nil
}

// AttachToGroup attaches policy to the group.
func (s *InMemoryStore) AttachToGroup(_ context.Context, group, policy string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.grpToPol[group] == nil {
		s.grpToPol[group] = make(map[string]struct{})
	}
	s.grpToPol[group][policy] = struct{}{}
	return nil
}

// DetachFromGroup removes policy from the group.
// Detaching a policy that was never attached is a no-op.
func (s *InMemoryStore) DetachFromGroup(_ context.Context, group, policy string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.grpToPol[group] != nil {
		delete(s.grpToPol[group], policy)
	}
	return nil
}

// GroupPolicies returns the list of policies directly attached to group.
func (s *InMemoryStore) GroupPolicies(_ context.Context, group string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.grpToPol[group]))
	for p := range s.grpToPol[group] {
		out = append(out, p)
	}
	return out, nil
}
