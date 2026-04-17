# Cache Invalidation Flow Architecture

**Week 1 Design Document**

## Problem Statement

When S3 PUT creates an object, the NFS server's VFS stat/dir caches show stale data. We need a mechanism for S3 mutations to invalidate VFS caches.

## Current Architecture

```
S3 Client → S3 Handler → CachedBackend → DistributedBackend → Raft → FSM Apply
                                                     ↓
                                            OnApply callback
                                                     ↓
                                         CachedBackend.InvalidateKey()
                                                     ↓
                                           (S3 cache invalidated)

NFS Server → VFS (local instance) → stat/dir caches (stale!)
```

**Problem:** NFS server creates its own VFS instance with local caches. The OnApply callback only invalidates S3's `CachedBackend`, not VFS caches.

## Design Decisions

### Decision 1: VFS Instance Registration

**Approach:** VFS instances register themselves with a central registry on creation. `DistributedBackend.onApply` iterates through registered VFS instances and invalidates their caches.

**Rationale:**
- Keeps `DistributedBackend` in control of invalidation flow
- VFS package doesn't need to know about cluster package
- Loose coupling via interface

**Data Structures:**

```go
// In cluster package
type CacheInvalidator interface {
    Invalidate(bucket, key string)
}

type DistributedBackend struct {
    // ... existing fields ...
    cacheInvalidators sync.Map // volumeID → CacheInvalidator
}

func (b *DistributedBackend) RegisterCacheInvalidator(volumeID string, invalidator CacheInvalidator) {
    b.cacheInvalidators.Store(volumeID, invalidator)
}
```

```go
// In vfs package
type GrainVFS struct {
    // ... existing fields ...
    volumeID string
}

func (fs *GrainVFS) Invalidate(bucket, key string) {
    // Convert S3 bucket/key to VFS file path
    // S3: bucket="default", key="path/to/file.txt"
    // VFS: fp="path/to/file.txt" (within volume's bucket)

    // For volumes that map directly to S3 buckets:
    if fs.bucket == "__grainfs_vfs_"+bucket {
        fs.invalidateStatCache(key)
        fs.invalidateParentDirCache(key)
    }
}
```

### Decision 2: Bucket/Key to File Path Mapping

**Challenge:** VFS uses file paths (e.g., "path/to/file.txt") while S3 uses bucket/key (e.g., bucket="default", key="path/to/file.txt").

**Approach:** For direct volume-to-bucket mapping, strip the VFS bucket prefix to extract the file path component.

**Example:**
- S3: `bucket="default", key="datasets/image.jpg"`
- VFS bucket: `"__grainfs_vfs_default"`
- VFS file path: `"datasets/image.jpg"`
- Invalidation: `vfs.invalidateStatCache("datasets/image.jpg")`

### Decision 3: Eventual Consistency Window

**Acceptable Inconsistency:** Raft commit latency (~50-100ms in LAN)

**Implementation:**
- VFS caches use short TTLs (1s stat, 5s dir) for POSIX semantics
- Cache invalidation happens immediately after Raft apply
- If invalidation delayed, VFS TTL ensures eventual consistency

### Decision 4: Deleted File Tracking for ESTALE

**Requirement:** NFS must return `ESTALE` error when accessing deleted files.

**Design:**

```go
// In VFS
type GrainVFS struct {
    // ... existing fields ...
    deletedCache *lru.Cache // bucket+key → bool (fast path)
    db           *badger.DB  // persistent deleted file tracking
}

func (fs *GrainVFS) MarkDeleted(bucket, key string) error {
    // Write to BadgerDB for persistence
    cacheKey := "deleted:" + bucket + ":" + key
    return fs.db.Update(func(txn *badger.Txn) error {
        return txn.Set([]byte(cacheKey), []byte("1"))
    })
}

func (fs *GrainVFS) IsDeleted(bucket, key string) bool {
    // Check cache first
    if _, ok := fs.deletedCache.Get(bucket + ":" + key); ok {
        return true
    }
    // Check BadgerDB
    // ...
}
```

**NFS ESTALE Propagation:**

```go
// In nfsserver handler
func (h *Handler) GetAttr(ctx context.Context, f nfs.File) (nfs.FileAttr, error) {
    attr, err := h.fs.Stat(ctx, f.Path())
    if h.fs.IsDeleted(volume, bucket, f.Path()) {
        return nfs.FileAttr{}, nfs.ErrStale
    }
    return attr, err
}
```

## Open Questions

1. **Volume-to-Bucket Mapping:** Does each volume map 1:1 to a bucket? Or can one volume span multiple buckets?

2. **Multi-Node Invalidation:** In cluster mode, should invalidation messages be broadcast via Raft, or should each node's OnApply callback handle local invalidation only?

   **Answer:** Each node's OnApply callback handles local invalidation. Raft replicates the mutation command to all nodes, and each node independently applies it and invalidates its local caches.

3. **NFS Server Lifecycle:** If NFS server restarts, how does it re-register its VFS instance?

   **Answer:** NFS server calls `RegisterCacheInvalidator()` in `ListenAndServe()` when creating the VFS instance.

## Next Steps

Week 3-4: Implement based on this design
- Add `CacheInvalidator` interface to cluster package
- Implement VFS registration mechanism
- Add VFS `Invalidate()` and `MarkDeleted()` methods
- Wire up OnApply callback to iterate through registered VFS instances
- Implement ESTALE error propagation in NFS server
