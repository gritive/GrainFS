package storage

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

const (
	defaultMaxCacheBytes       = 64 * 1024 * 1024 // 64MB total cache
	defaultMaxObjectCacheBytes = 4 * 1024 * 1024  // 4MB per object
)

// cacheEntry stores cached object content and metadata.
type cacheEntry struct {
	data []byte
	obj  Object // copy, not pointer
	size int64
}

// CacheStats reports cache performance counters.
type CacheStats struct {
	Hits      int64
	Misses    int64
	Evictions int64
}

// CacheOption configures CachedBackend.
type CacheOption func(*CachedBackend)

// WithMaxCacheBytes sets the total maximum bytes for cached object content.
func WithMaxCacheBytes(n int64) CacheOption {
	return func(cb *CachedBackend) { cb.maxBytes = n }
}

// WithMaxObjectCacheBytes sets the maximum bytes for a single cached object.
func WithMaxObjectCacheBytes(n int64) CacheOption {
	return func(cb *CachedBackend) { cb.maxObjBytes = n }
}

// CachedBackend wraps a Backend with an LRU read cache for object content
// and metadata. Writes invalidate the corresponding cache entries.
type CachedBackend struct {
	Backend // embedded, all methods delegated by default

	maxBytes    int64
	maxObjBytes int64

	mu        sync.RWMutex
	entries   map[string]*lruNode
	head      *lruNode // most recently used
	tail      *lruNode // least recently used
	usedBytes int64

	hits      atomic.Int64
	misses    atomic.Int64
	evictions atomic.Int64
}

// lruNode is a doubly-linked list node for LRU eviction.
type lruNode struct {
	key  string
	val  cacheEntry
	prev *lruNode
	next *lruNode
}

// NewCachedBackend creates a caching wrapper around the given backend.
func NewCachedBackend(backend Backend, opts ...CacheOption) *CachedBackend {
	cb := &CachedBackend{
		Backend:     backend,
		maxBytes:    defaultMaxCacheBytes,
		maxObjBytes: defaultMaxObjectCacheBytes,
		entries:     make(map[string]*lruNode),
	}
	for _, o := range opts {
		o(cb)
	}
	return cb
}

// Stats returns a snapshot of cache performance counters.
func (cb *CachedBackend) Stats() CacheStats {
	return CacheStats{
		Hits:      cb.hits.Load(),
		Misses:    cb.misses.Load(),
		Evictions: cb.evictions.Load(),
	}
}

func cacheKey(bucket, key string) string { return bucket + "/" + key }

// GetObject returns a cached reader on hit, or fetches from the underlying
// backend and caches the content on miss.
func (cb *CachedBackend) GetObject(bucket, key string) (io.ReadCloser, *Object, error) {
	ck := cacheKey(bucket, key)

	// Fast path: cache hit — hold write lock to safely read node data and update LRU
	cb.mu.Lock()
	node, ok := cb.entries[ck]
	if ok {
		cb.moveToFront(node)
		data := node.val.data
		obj := node.val.obj // copy
		cb.mu.Unlock()
		cb.hits.Add(1)
		return io.NopCloser(bytes.NewReader(data)), &obj, nil
	}
	cb.mu.Unlock()

	// Cache miss: check size before buffering
	meta, err := cb.Backend.HeadObject(bucket, key)
	if err != nil {
		return nil, nil, err
	}

	rc, obj, err := cb.Backend.GetObject(bucket, key)
	if err != nil {
		return nil, nil, err
	}

	cb.misses.Add(1)

	// Large objects: return streaming reader without buffering
	if meta.Size > cb.maxObjBytes {
		return rc, obj, nil
	}

	// Small objects: buffer and cache
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return nil, nil, err
	}

	cb.put(ck, cacheEntry{data: data, obj: *obj, size: int64(len(data))})

	return io.NopCloser(bytes.NewReader(data)), obj, nil
}

// HeadObject returns cached metadata on hit.
func (cb *CachedBackend) HeadObject(bucket, key string) (*Object, error) {
	ck := cacheKey(bucket, key)

	cb.mu.Lock()
	node, ok := cb.entries[ck]
	if ok {
		cb.moveToFront(node)
		obj := node.val.obj // copy
		cb.mu.Unlock()
		return &obj, nil
	}
	cb.mu.Unlock()

	return cb.Backend.HeadObject(bucket, key)
}

// PutObject invalidates the cache entry for the key.
func (cb *CachedBackend) PutObject(bucket, key string, r io.Reader, contentType string) (*Object, error) {
	cb.invalidate(bucket, key)
	return cb.Backend.PutObject(bucket, key, r, contentType)
}

// DeleteObject invalidates the cache entry for the key.
func (cb *CachedBackend) DeleteObject(bucket, key string) error {
	cb.invalidate(bucket, key)
	return cb.Backend.DeleteObject(bucket, key)
}

// DeleteObjectReturningMarker invalidates the cache and delegates to the inner backend.
// This ensures the cache is evicted even when the versioned soft-delete path is taken.
func (cb *CachedBackend) DeleteObjectReturningMarker(bucket, key string) (string, error) {
	cb.invalidate(bucket, key)
	type versionedSoftDeleter interface {
		DeleteObjectReturningMarker(bucket, key string) (string, error)
	}
	if vsd, ok := cb.Backend.(versionedSoftDeleter); ok {
		return vsd.DeleteObjectReturningMarker(bucket, key)
	}
	return "", cb.Backend.DeleteObject(bucket, key)
}

// DeleteObjectVersion invalidates the cache entry and hard-deletes the version.
func (cb *CachedBackend) DeleteObjectVersion(bucket, key, versionID string) error {
	cb.invalidate(bucket, key)
	type versionDeleter interface {
		DeleteObjectVersion(bucket, key, versionID string) error
	}
	if vd, ok := cb.Backend.(versionDeleter); ok {
		return vd.DeleteObjectVersion(bucket, key, versionID)
	}
	return fmt.Errorf("cached: inner backend does not support DeleteObjectVersion")
}

// CompleteMultipartUpload invalidates the cache entry for the key.
func (cb *CachedBackend) CompleteMultipartUpload(bucket, key, uploadID string, parts []Part) (*Object, error) {
	cb.invalidate(bucket, key)
	return cb.Backend.CompleteMultipartUpload(bucket, key, uploadID, parts)
}

// Close closes the underlying backend (e.g., flushing BadgerDB).
func (cb *CachedBackend) Close() error {
	if closer, ok := cb.Backend.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

// Unwrap returns the underlying Backend, allowing callers to access
// backend-specific interfaces via type assertion.
func (cb *CachedBackend) Unwrap() Backend {
	return cb.Backend
}

// ListAllObjects implements Snapshotable by delegating to the inner backend.
func (cb *CachedBackend) ListAllObjects() ([]SnapshotObject, error) {
	if snap, ok := cb.Backend.(Snapshotable); ok {
		return snap.ListAllObjects()
	}
	return nil, ErrSnapshotNotSupported
}

// RestoreObjects implements Snapshotable and invalidates the full cache after restore.
func (cb *CachedBackend) RestoreObjects(objects []SnapshotObject) (int, []StaleBlob, error) {
	if snap, ok := cb.Backend.(Snapshotable); ok {
		count, stale, err := snap.RestoreObjects(objects)
		if err == nil {
			cb.mu.Lock()
			cb.entries = make(map[string]*lruNode)
			cb.head, cb.tail = nil, nil
			cb.usedBytes = 0
			cb.mu.Unlock()
		}
		return count, stale, err
	}
	return 0, nil, ErrSnapshotNotSupported
}

// ListAllBuckets implements BucketSnapshotable by delegating to the inner backend.
func (cb *CachedBackend) ListAllBuckets() ([]SnapshotBucket, error) {
	if bs, ok := cb.Backend.(BucketSnapshotable); ok {
		return bs.ListAllBuckets()
	}
	return nil, nil
}

// RestoreBuckets implements BucketSnapshotable by delegating to the inner backend.
func (cb *CachedBackend) RestoreBuckets(buckets []SnapshotBucket) error {
	if bs, ok := cb.Backend.(BucketSnapshotable); ok {
		return bs.RestoreBuckets(buckets)
	}
	return nil
}

// InvalidateKey removes a cache entry for the given bucket/key.
// This is the public API used by cluster cache invalidation (OnApply callback).
func (cb *CachedBackend) InvalidateKey(bucket, key string) {
	cb.invalidate(bucket, key)
}

// invalidate removes a key from the cache.
func (cb *CachedBackend) invalidate(bucket, key string) {
	ck := cacheKey(bucket, key)
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if node, ok := cb.entries[ck]; ok {
		cb.removeNode(node)
		delete(cb.entries, ck)
		cb.usedBytes -= node.val.size
	}
}

// put adds an entry to the cache, evicting LRU entries if necessary.
func (cb *CachedBackend) put(key string, entry cacheEntry) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// If already exists, update
	if node, ok := cb.entries[key]; ok {
		cb.usedBytes -= node.val.size
		node.val = entry
		cb.usedBytes += entry.size
		cb.moveToFront(node)
		return
	}

	// Evict until we have room
	for cb.usedBytes+entry.size > cb.maxBytes && cb.tail != nil {
		cb.evictLRU()
	}

	node := &lruNode{key: key, val: entry}
	cb.entries[key] = node
	cb.usedBytes += entry.size
	cb.addToFront(node)
}

func (cb *CachedBackend) evictLRU() {
	if cb.tail == nil {
		return
	}
	victim := cb.tail
	cb.removeNode(victim)
	delete(cb.entries, victim.key)
	cb.usedBytes -= victim.val.size
	cb.evictions.Add(1)
}

func (cb *CachedBackend) addToFront(node *lruNode) {
	node.prev = nil
	node.next = cb.head
	if cb.head != nil {
		cb.head.prev = node
	}
	cb.head = node
	if cb.tail == nil {
		cb.tail = node
	}
}

func (cb *CachedBackend) removeNode(node *lruNode) {
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		cb.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		cb.tail = node.prev
	}
	node.prev = nil
	node.next = nil
}

func (cb *CachedBackend) moveToFront(node *lruNode) {
	if cb.head == node {
		return
	}
	cb.removeNode(node)
	cb.addToFront(node)
}
