package protocred

import (
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
