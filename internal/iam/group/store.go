package group

import (
	"context"
	"errors"
	"sync"
)

var ErrGroupNotFound = errors.New("group not found")

type Group struct {
	Name             string
	AttachedPolicies []string
	Members          map[string]struct{}
}

type InMemoryStore struct {
	mu sync.RWMutex
	g  map[string]*Group
}

func NewInMemoryStore() *InMemoryStore { return &InMemoryStore{g: make(map[string]*Group)} }

// Put creates the group if absent, or replaces AttachedPolicies if present.
// Member list is preserved on update; use AddMember/RemoveMember to mutate it.
func (s *InMemoryStore) Put(_ context.Context, name string, policies []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if g, ok := s.g[name]; ok {
		g.AttachedPolicies = append([]string(nil), policies...)
		return nil
	}
	s.g[name] = &Group{Name: name, AttachedPolicies: append([]string(nil), policies...), Members: make(map[string]struct{})}
	return nil
}

func (s *InMemoryStore) Delete(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.g[name]; !ok {
		return ErrGroupNotFound
	}
	delete(s.g, name)
	return nil
}

func (s *InMemoryStore) AddMember(_ context.Context, groupName, saID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.g[groupName]
	if !ok {
		return ErrGroupNotFound
	}
	g.Members[saID] = struct{}{}
	return nil
}

func (s *InMemoryStore) RemoveMember(_ context.Context, groupName, saID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.g[groupName]
	if !ok {
		return ErrGroupNotFound
	}
	delete(g.Members, saID)
	return nil
}

// MembershipOf returns the list of group names containing saID.
func (s *InMemoryStore) MembershipOf(_ context.Context, saID string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for n, g := range s.g {
		if _, ok := g.Members[saID]; ok {
			out = append(out, n)
		}
	}
	return out, nil
}

func (s *InMemoryStore) AttachedPolicies(_ context.Context, groupName string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	g, ok := s.g[groupName]
	if !ok {
		return nil, ErrGroupNotFound
	}
	return append([]string(nil), g.AttachedPolicies...), nil
}

// MembersOf returns the list of sa_id's in groupName, for FSM-side resolver invalidation.
func (s *InMemoryStore) MembersOf(_ context.Context, groupName string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	g, ok := s.g[groupName]
	if !ok {
		return nil, ErrGroupNotFound
	}
	out := make([]string, 0, len(g.Members))
	for sa := range g.Members {
		out = append(out, sa)
	}
	return out, nil
}

// GroupEntry is a snapshot representation of one group store entry.
type GroupEntry struct {
	Name             string
	AttachedPolicies []string
	Members          []string
}

// Snapshot returns a copy of all groups for serialization.
func (s *InMemoryStore) Snapshot() []GroupEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]GroupEntry, 0, len(s.g))
	for _, g := range s.g {
		members := make([]string, 0, len(g.Members))
		for m := range g.Members {
			members = append(members, m)
		}
		out = append(out, GroupEntry{
			Name:             g.Name,
			AttachedPolicies: append([]string(nil), g.AttachedPolicies...),
			Members:          members,
		})
	}
	return out
}

// ReplaceAll atomically replaces all group entries with the provided snapshot.
func (s *InMemoryStore) ReplaceAll(entries []GroupEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.g = make(map[string]*Group, len(entries))
	for _, e := range entries {
		members := make(map[string]struct{}, len(e.Members))
		for _, m := range e.Members {
			members[m] = struct{}{}
		}
		s.g[e.Name] = &Group{
			Name:             e.Name,
			AttachedPolicies: append([]string(nil), e.AttachedPolicies...),
			Members:          members,
		}
	}
}
