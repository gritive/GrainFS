package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/gritive/GrainFS/internal/pool"
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

type objectCacheKey struct {
	bucket string
	key    string
}

var cachedObjectReaderStatePool = pool.New(func() *cachedObjectReaderState {
	return &cachedObjectReaderState{}
})

type cachedObjectReader struct {
	st *cachedObjectReaderState
}

type cachedObjectReaderState struct {
	r bytes.Reader
}

func newCachedObjectReader(data []byte) *cachedObjectReader {
	st := cachedObjectReaderStatePool.Get()
	st.r.Reset(data)
	return &cachedObjectReader{st: st}
}

func (r *cachedObjectReader) Read(p []byte) (int, error) {
	if r.st == nil {
		return 0, io.ErrClosedPipe
	}
	return r.st.r.Read(p)
}

func (r *cachedObjectReader) WriteTo(w io.Writer) (int64, error) {
	if r.st == nil {
		return 0, io.ErrClosedPipe
	}
	return r.st.r.WriteTo(w)
}

func (r *cachedObjectReader) Close() error {
	st := r.st
	if st == nil {
		return nil
	}
	r.st = nil
	st.r.Reset(nil)
	cachedObjectReaderStatePool.Put(st)
	return nil
}

// CacheStats reports cache performance counters.
type CacheStats struct {
	Hits      int64
	Misses    int64
	Evictions int64
}

// CacheOption configures CachedBackend.
type CacheOption func(*CachedBackend)

var (
	_ Backend     = (*CachedBackend)(nil)
	_ PartialIO   = (*CachedBackend)(nil)
	_ io.WriterTo = (*cachedObjectReader)(nil)
)

// WithMaxCacheBytes sets the total maximum bytes for cached object content.
func WithMaxCacheBytes(n int64) CacheOption {
	return func(cb *CachedBackend) { cb.maxBytes = n }
}

// WithMaxObjectCacheBytes sets the maximum bytes for a single cached object.
func WithMaxObjectCacheBytes(n int64) CacheOption {
	return func(cb *CachedBackend) { cb.maxObjBytes = n }
}

// CachedBackend wraps a Backend with a bounded read cache for object content
// and metadata. Cache state is published as immutable atomic snapshots:
// reads do not take locks, while writes clone and CAS the next snapshot.
// Writes invalidate the corresponding cache entries.
type CachedBackend struct {
	Backend // embedded, all methods delegated by default

	maxBytes    int64
	maxObjBytes int64

	state atomic.Pointer[cacheState]

	hits      atomic.Int64
	misses    atomic.Int64
	evictions atomic.Int64
}

// cacheState is immutable after publication through CachedBackend.state.
// Readers load and inspect it without locks; writers clone, mutate, and publish
// with CompareAndSwap.
type cacheState struct {
	entries   map[objectCacheKey]cacheEntry
	order     []objectCacheKey // oldest -> newest
	usedBytes int64
}

// NewCachedBackend creates a caching wrapper around the given backend.
func NewCachedBackend(backend Backend, opts ...CacheOption) *CachedBackend {
	cb := &CachedBackend{
		Backend:     backend,
		maxBytes:    defaultMaxCacheBytes,
		maxObjBytes: defaultMaxObjectCacheBytes,
	}
	cb.state.Store(emptyCacheState())
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

func cacheKey(bucket, key string) objectCacheKey { return objectCacheKey{bucket: bucket, key: key} }

func emptyCacheState() *cacheState {
	return &cacheState{entries: make(map[objectCacheKey]cacheEntry)}
}

// GetObject returns a cached reader on hit, or fetches from the underlying
// backend and caches the content on miss.
func (cb *CachedBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *Object, error) {
	ck := cacheKey(bucket, key)

	// Fast path: cache hit from immutable atomic snapshot.
	entry, ok := cb.getCached(ck)
	if ok {
		cb.hits.Add(1)
		obj := entry.obj // copy
		return newCachedObjectReader(entry.data), &obj, nil
	}

	// Cache miss: check size before buffering
	meta, err := cb.Backend.HeadObject(ctx, bucket, key)
	if err != nil {
		return nil, nil, err
	}

	rc, obj, err := cb.Backend.GetObject(ctx, bucket, key)
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

	return newCachedObjectReader(data), obj, nil
}

// HeadObject returns cached metadata on hit.
func (cb *CachedBackend) HeadObject(ctx context.Context, bucket, key string) (*Object, error) {
	ck := cacheKey(bucket, key)

	entry, ok := cb.getCached(ck)
	if ok {
		obj := entry.obj // copy
		return &obj, nil
	}

	return cb.Backend.HeadObject(ctx, bucket, key)
}

func (cb *CachedBackend) SetObjectACL(bucket, key string, acl uint8) error {
	cb.invalidate(bucket, key)
	setter, ok := cb.Backend.(ACLSetter)
	if !ok {
		return UnsupportedOperationError{Op: "SetObjectACL", Reason: UnsupportedReasonNoAdapter}
	}
	return setter.SetObjectACL(bucket, key, acl)
}

// PutObject invalidates the cache entry for the key.
func (cb *CachedBackend) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error) {
	cb.invalidate(bucket, key)
	return cb.Backend.PutObject(ctx, bucket, key, r, contentType)
}

func (cb *CachedBackend) PutObjectWithUserMetadata(ctx context.Context, bucket, key string, r io.Reader, contentType string, userMetadata map[string]string) (*Object, error) {
	cb.invalidate(bucket, key)
	putter, ok := cb.Backend.(UserMetadataPutter)
	if !ok {
		return nil, UnsupportedOperationError{Op: "PutObjectWithUserMetadata", Reason: UnsupportedReasonNoAdapter}
	}
	return putter.PutObjectWithUserMetadata(ctx, bucket, key, r, contentType, userMetadata)
}

func (cb *CachedBackend) PutObjectWithACL(bucket, key string, r io.Reader, contentType string, acl uint8) (*Object, error) {
	cb.invalidate(bucket, key)
	return putObjectWithACLOnBackend(context.Background(), cb.Backend, bucket, key, r, contentType, acl)
}

// PutObjectAsync delegates to the inner backend's async write-back path if available.
// The cache entry is invalidated immediately so reads after write don't return stale data.
func (cb *CachedBackend) PutObjectAsync(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, func() error, error) {
	type asyncPutter interface {
		PutObjectAsync(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, func() error, error)
	}
	ap, ok := cb.Backend.(asyncPutter)
	if !ok {
		obj, err := cb.PutObject(ctx, bucket, key, r, contentType)
		return obj, func() error { return nil }, err
	}
	cb.invalidate(bucket, key)
	return ap.PutObjectAsync(ctx, bucket, key, r, contentType)
}

// WriteAt delegates to the inner backend's WriteAt if available, then invalidates
// the cache entry so subsequent reads fetch the updated content.
func (cb *CachedBackend) WriteAt(ctx context.Context, bucket, key string, offset uint64, data []byte) (*Object, error) {
	wa, ok := cb.Backend.(PartialIO)
	if !ok {
		return nil, fmt.Errorf("inner backend does not support WriteAt")
	}
	cb.invalidate(bucket, key)
	return wa.WriteAt(ctx, bucket, key, offset, data)
}

// ReadAt serves pread from the in-memory cache on hit; delegates to inner on miss.
// For large objects (not cached), bypasses GetObject/Seek overhead entirely.
func (cb *CachedBackend) ReadAt(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, error) {
	ck := cacheKey(bucket, key)
	entry, ok := cb.getCached(ck)
	if ok {
		var r bytes.Reader
		r.Reset(entry.data)
		return r.ReadAt(buf, offset)
	}

	ra, ok := cb.Backend.(PartialIO)
	if !ok {
		return 0, fmt.Errorf("inner backend does not support ReadAt")
	}
	return ra.ReadAt(ctx, bucket, key, offset, buf)
}

func (cb *CachedBackend) PreferReadAt(bucket string) bool {
	type readAtPreference interface {
		PreferReadAt(bucket string) bool
	}
	pref, ok := cb.Backend.(readAtPreference)
	return ok && pref.PreferReadAt(bucket)
}

func (cb *CachedBackend) PreferWriteAt(bucket string) bool {
	type writeAtPreference interface {
		PreferWriteAt(bucket string) bool
	}
	pref, ok := cb.Backend.(writeAtPreference)
	return ok && pref.PreferWriteAt(bucket)
}

// Truncate delegates to the inner backend's Truncate if available, then
// invalidates the cache entry so subsequent reads fetch the updated content.
func (cb *CachedBackend) Truncate(ctx context.Context, bucket, key string, size int64) error {
	tr, ok := cb.Backend.(Truncatable)
	if !ok {
		return fmt.Errorf("inner backend does not support Truncate")
	}
	cb.invalidate(bucket, key)
	return tr.Truncate(ctx, bucket, key, size)
}

// DeleteObject invalidates the cache entry for the key.
func (cb *CachedBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	cb.invalidate(bucket, key)
	return cb.Backend.DeleteObject(ctx, bucket, key)
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
	return "", cb.Backend.DeleteObject(context.Background(), bucket, key)
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
func (cb *CachedBackend) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []Part) (*Object, error) {
	cb.invalidate(bucket, key)
	return cb.Backend.CompleteMultipartUpload(ctx, bucket, key, uploadID, parts)
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
			cb.state.Store(emptyCacheState())
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
	for {
		cur := cb.state.Load()
		if _, ok := cur.entries[ck]; !ok {
			return
		}
		next := cur.cloneWithout(ck)
		if cb.state.CompareAndSwap(cur, next) {
			return
		}
	}
}

// put adds an entry to the cache, evicting the oldest published entries if necessary.
func (cb *CachedBackend) put(key objectCacheKey, entry cacheEntry) {
	if entry.size > cb.maxBytes {
		return
	}
	for {
		cur := cb.state.Load()
		next, evicted := cur.cloneWith(key, entry, cb.maxBytes)
		if cb.state.CompareAndSwap(cur, next) {
			if evicted > 0 {
				cb.evictions.Add(evicted)
			}
			return
		}
	}
}

func (cb *CachedBackend) getCached(key objectCacheKey) (cacheEntry, bool) {
	st := cb.state.Load()
	if st == nil {
		return cacheEntry{}, false
	}
	entry, ok := st.entries[key]
	return entry, ok
}

func (s *cacheState) cloneWithout(key objectCacheKey) *cacheState {
	next := &cacheState{
		entries:   make(map[objectCacheKey]cacheEntry, len(s.entries)-1),
		order:     make([]objectCacheKey, 0, len(s.order)),
		usedBytes: s.usedBytes,
	}
	for k, v := range s.entries {
		if k == key {
			next.usedBytes -= v.size
			continue
		}
		next.entries[k] = v
	}
	for _, k := range s.order {
		if k != key {
			next.order = append(next.order, k)
		}
	}
	return next
}

func (s *cacheState) cloneWith(key objectCacheKey, entry cacheEntry, maxBytes int64) (*cacheState, int64) {
	next := &cacheState{
		entries:   make(map[objectCacheKey]cacheEntry, len(s.entries)+1),
		order:     make([]objectCacheKey, 0, len(s.order)+1),
		usedBytes: s.usedBytes,
	}
	for k, v := range s.entries {
		next.entries[k] = v
	}
	for _, k := range s.order {
		if k != key {
			next.order = append(next.order, k)
		}
	}
	if old, ok := next.entries[key]; ok {
		next.usedBytes -= old.size
	}
	next.entries[key] = entry
	next.order = append(next.order, key)
	next.usedBytes += entry.size

	var evicted int64
	for next.usedBytes > maxBytes && len(next.order) > 0 {
		victim := next.order[0]
		next.order = next.order[1:]
		if victim == key && len(next.order) == 0 {
			break
		}
		if old, ok := next.entries[victim]; ok {
			delete(next.entries, victim)
			next.usedBytes -= old.size
			evicted++
		}
	}
	return next, evicted
}
