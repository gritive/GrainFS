package cluster

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
	r.invalidators[volumeID] = invalidator
}

// InvalidateAll calls Invalidate on all registered invalidators.
func (r *Registry) InvalidateAll(bucket, key string) {
	for _, invalidator := range r.invalidators {
		invalidator.Invalidate(bucket, key)
	}
}
