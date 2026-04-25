package cluster

import (
	"sync"

	"github.com/gritive/GrainFS/internal/metrics"
)

// CacheInvalidator is implemented by components that maintain caches
// which must be invalidated when objects are mutated via Raft.
//
// Implementations: VFS (stat/dir caches), S3 CachedBackend (object cache)
//
// Called by DistributedBackend.onApply after Raft commits a mutation.
type CacheInvalidator interface {
	// Invalidate clears caches for the given bucket and key.
	//
	// For S3: bucket="mybucket", key="path/to/file.txt"
	// For VFS: Must map to internal file path based on volume configuration
	//
	// Called after: PutObject, DeleteObject, CompleteMultipartUpload
	Invalidate(bucket, key string)
}

// Registry manages cache invalidators across all protocols.
// Used by DistributedBackend to broadcast invalidation events.
type Registry struct {
	mu           sync.RWMutex
	invalidators map[string]CacheInvalidator // volumeID → invalidator
}

// NewRegistry creates a new cache invalidator registry.
func NewRegistry() *Registry {
	return &Registry{
		invalidators: make(map[string]CacheInvalidator),
	}
}

// Register adds a cache invalidator for a specific volume.
func (r *Registry) Register(volumeID string, invalidator CacheInvalidator) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.invalidators[volumeID] = invalidator
	r.updateSizeMetric()
}

// InvalidateAll calls Invalidate on all registered invalidators concurrently.
//
// Note: this reduces Apply loop block time from Σ(per-invalidator) to
// max(per-invalidator). The Apply loop still blocks at wg.Wait() —
// it's a reduction, not an elimination.
func (r *Registry) InvalidateAll(bucket, key string) {
	r.mu.RLock()
	invs := make([]CacheInvalidator, 0, len(r.invalidators))
	for _, inv := range r.invalidators {
		invs = append(invs, inv)
	}
	r.mu.RUnlock()

	if len(invs) == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, inv := range invs {
		inv := inv
		wg.Add(1)
		go func() {
			defer wg.Done()
			inv.Invalidate(bucket, key)
		}()
	}
	wg.Wait()
}

// GetInvalidator returns a registered invalidator by volume ID.
func (r *Registry) GetInvalidator(volumeID string) CacheInvalidator {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.invalidators[volumeID]
}

// GetInvalidators returns a snapshot of all registered invalidators.
func (r *Registry) GetInvalidators() map[string]CacheInvalidator {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make(map[string]CacheInvalidator, len(r.invalidators))
	for k, v := range r.invalidators {
		result[k] = v
	}
	return result
}

// updateSizeMetric updates the registry size metric.
func (r *Registry) updateSizeMetric() {
	metrics.RegistrySize.Set(float64(len(r.invalidators)))
}
