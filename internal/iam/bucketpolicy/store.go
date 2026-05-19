package bucketpolicy

import (
	"context"
	"errors"
	"sync"
)

// ErrNotFound is returned when a bucket has no policy document stored.
var ErrNotFound = errors.New("bucket policy not found")

// InMemoryStore holds bucket-scoped policy documents as raw bytes.
// All methods are safe for concurrent use.
type InMemoryStore struct {
	mu sync.RWMutex
	m  map[string][]byte
}

// NewInMemoryStore returns an empty InMemoryStore.
func NewInMemoryStore() *InMemoryStore { return &InMemoryStore{m: make(map[string][]byte)} }

// Put stores or replaces the policy document for bucket.
// The stored bytes are an independent copy of raw.
func (s *InMemoryStore) Put(_ context.Context, bucket string, raw []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[bucket] = append([]byte(nil), raw...)
	return nil
}

// Get returns the policy document for bucket.
// The returned bytes are an independent copy; callers may mutate them freely.
// Returns ErrNotFound if no policy has been stored for bucket.
func (s *InMemoryStore) Get(_ context.Context, bucket string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[bucket]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), v...), nil
}

// Delete removes the policy document for bucket.
// Returns ErrNotFound if no policy was stored.
func (s *InMemoryStore) Delete(_ context.Context, bucket string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.m[bucket]; !ok {
		return ErrNotFound
	}
	delete(s.m, bucket)
	return nil
}
