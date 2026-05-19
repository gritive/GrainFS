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

// BucketPolicyEntry is a snapshot of one bucket's policy document.
type BucketPolicyEntry struct {
	Bucket string
	Doc    []byte
}

// Snapshot returns a copy of all bucket policy entries for serialization.
func (s *InMemoryStore) Snapshot() []BucketPolicyEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]BucketPolicyEntry, 0, len(s.m))
	for bucket, doc := range s.m {
		out = append(out, BucketPolicyEntry{
			Bucket: bucket,
			Doc:    append([]byte(nil), doc...),
		})
	}
	return out
}

// ReplaceAll atomically replaces all bucket policy entries with the provided snapshot.
func (s *InMemoryStore) ReplaceAll(entries []BucketPolicyEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m = make(map[string][]byte, len(entries))
	for _, e := range entries {
		s.m[e.Bucket] = append([]byte(nil), e.Doc...)
	}
}
