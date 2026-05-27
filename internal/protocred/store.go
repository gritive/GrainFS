package protocred

import (
	"sort"
	"sync"
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
