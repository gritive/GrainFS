package server

import (
	"sync"
)

// PolicyStore manages bucket policies with an in-memory cache.
// Policies are stored in the storage backend and cached here.
type PolicyStore struct {
	mu       sync.RWMutex
	policies map[string]*BucketPolicy // bucket -> parsed policy
	raw      map[string][]byte        // bucket -> raw JSON (for GET)
}

// NewPolicyStore creates a new empty policy store.
func NewPolicyStore() *PolicyStore {
	return &PolicyStore{
		policies: make(map[string]*BucketPolicy),
		raw:      make(map[string][]byte),
	}
}

// Get returns the policy for a bucket, or nil if none exists.
func (ps *PolicyStore) Get(bucket string) *BucketPolicy {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.policies[bucket]
}

// Set parses and caches a policy for a bucket.
func (ps *PolicyStore) Set(bucket string, policyJSON []byte) error {
	p, err := ParsePolicy(policyJSON)
	if err != nil {
		return err
	}
	ps.mu.Lock()
	ps.policies[bucket] = p
	ps.raw[bucket] = policyJSON
	ps.mu.Unlock()
	return nil
}

// GetRaw returns the raw JSON policy for a bucket, or nil.
func (ps *PolicyStore) GetRaw(bucket string) []byte {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.raw[bucket]
}

// Delete removes the cached policy for a bucket.
func (ps *PolicyStore) Delete(bucket string) {
	ps.mu.Lock()
	delete(ps.policies, bucket)
	delete(ps.raw, bucket)
	ps.mu.Unlock()
}

// IsAllowed checks if the access key can perform the action on the resource.
// If no policy exists for the bucket, returns true (no restriction).
// If a policy exists but doesn't allow the action, returns false (deny).
func (ps *PolicyStore) IsAllowed(accessKey, action, bucket, key string) bool {
	p := ps.Get(bucket)
	if p == nil {
		return true // no policy = no restriction
	}
	return p.IsAllowed(accessKey, action, bucket, key)
}
