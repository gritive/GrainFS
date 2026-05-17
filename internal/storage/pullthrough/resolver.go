package pullthrough

import (
	"crypto/sha256"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/iam"
)

// Resolver returns the per-bucket Upstream for pull-through caching. Callers
// invoke Resolve at request time; on (nil, false) the Backend treats the
// request as cache-miss-with-no-upstream and returns the original error
// (typically ErrObjectNotFound) without attempting any upstream fetch.
//
// Implementations must be safe for concurrent calls.
type Resolver interface {
	Resolve(bucket string) (Upstream, bool)
}

// IAMResolver implements Resolver against an *iam.Store. It builds a single
// *S3Upstream per bucket on first hit, caches it, and rebuilds when the
// (endpoint, access_key, secret_key) tuple changes (rotation or
// reconfiguration).
//
// The cache key includes a SHA-256 of SecretKeyEnc so that rotations that
// change ONLY the secret (same upstream URL, same AK — a normal AWS IAM
// rotation) still invalidate the cached client. Without it the equality
// check would still hold and the cached *S3Upstream would retain the OLD
// secret baked in by credentials.NewStaticCredentialsProvider, silently
// 401-ing every upstream GET until restart.
//
// Cache invalidation is lazy: every Resolve compares the cached tuple to
// the live IAM record. No FSM event subscription is needed — callers see
// the new client at most one request after the FSM apply.
//
// The cache is published as an immutable `map[string]*resolverEntry`
// snapshot via `atomic.Pointer`. Cache-hit reads are lock-free (single
// atomic load + map lookup). Mutations — cache fill on first hit, eviction
// when the record disappears, and rebuild on rotation — serialise on
// writeMu so concurrent mutators clone-on-write and publish a fresh
// pointer. S3Upstream construction happens under writeMu so a single
// upstream client is built per rotation; readers piling up on the rotated
// bucket all see the same final pointer once the writer stores it.
//
// Audit follow-up: docs/architecture/lock-free-audit.md →
// "internal/storage/pullthrough/resolver.go - upstream client cache; hits
// take read lock, rotations rebuild under write lock." Every pull-through
// S3 request was paying that RLock; atomic publication eliminates it.
type IAMResolver struct {
	store   *iam.Store
	writeMu sync.Mutex
	cache   atomic.Pointer[map[string]*resolverEntry]
}

type resolverEntry struct {
	endpoint         string
	accessKey        string
	secretKeyEncHash [32]byte
	upstream         *S3Upstream
}

// NewIAMResolver returns a Resolver backed by the given IAM Store. The store
// must be non-nil; nil panics on first Resolve. Callers should fail-fast in
// the construction path rather than relying on this guard (see runtime
// wiring in serveruntime/run.go).
func NewIAMResolver(store *iam.Store) *IAMResolver {
	r := &IAMResolver{store: store}
	initial := make(map[string]*resolverEntry)
	r.cache.Store(&initial)
	return r
}

// Resolve returns the *S3Upstream for the given bucket, building (or
// rebuilding on rotation) as needed. Returns (nil, false) when no upstream
// is configured for the bucket.
//
// Per /plan-eng-review override A4 — on NewS3Upstream build failure, a warning
// is logged before falling through to (nil, false). The handler then treats
// the result identically to "no upstream configured", but the operator sees
// the misconfiguration in node logs.
func (r *IAMResolver) Resolve(bucket string) (Upstream, bool) {
	rec, ok := r.store.LookupBucketUpstream(bucket)
	if !ok {
		// No upstream configured: evict any stale cache entry. Lock-free
		// probe first; only acquire writeMu if there is actually something
		// to remove.
		if _, hadEntry := (*r.cache.Load())[bucket]; hadEntry {
			r.evictLocked(bucket)
		}
		return nil, false
	}

	secretHash := sha256.Sum256(rec.SecretKeyEnc)

	// Fast path: cache hit with matching (endpoint, access_key, secret hash).
	// Single atomic load; no lock.
	if cached, hit := (*r.cache.Load())[bucket]; hit &&
		cached.endpoint == rec.Endpoint &&
		cached.accessKey == rec.AccessKey &&
		cached.secretKeyEncHash == secretHash {
		return cached.upstream, true
	}

	// Slow path: cache miss or rotation. Serialise builders so we only
	// construct one S3Upstream per rotation even under thundering-herd
	// readers, then publish via CoW.
	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	// Re-check after acquiring writeMu — another goroutine may have built
	// the same entry while we waited.
	old := r.cache.Load()
	if cached, hit := (*old)[bucket]; hit &&
		cached.endpoint == rec.Endpoint &&
		cached.accessKey == rec.AccessKey &&
		cached.secretKeyEncHash == secretHash {
		return cached.upstream, true
	}

	up, err := NewS3Upstream(rec.Endpoint, rec.AccessKey, rec.SecretKey)
	if err != nil {
		// A4: log the failure so operators see misconfigured records in node
		// logs even though the request path falls through silently.
		log.Warn().Err(err).
			Str("bucket", bucket).
			Str("endpoint", rec.Endpoint).
			Msg("pullthrough resolver: build upstream failed; bucket falls through")
		// Evict any stale entry so the next call doesn't return a fresher
		// build path against the same broken record without re-attempting.
		if _, hadEntry := (*old)[bucket]; hadEntry {
			next := cloneCacheWithout(old, bucket)
			r.cache.Store(next)
		}
		return nil, false
	}

	next := cloneCache(old, len(*old)+1)
	next[bucket] = &resolverEntry{
		endpoint:         rec.Endpoint,
		accessKey:        rec.AccessKey,
		secretKeyEncHash: secretHash,
		upstream:         up,
	}
	r.cache.Store(&next)
	return up, true
}

// evictLocked drops the entry for bucket from the cache under writeMu. The
// caller must have observed a hit on bucket via the lock-free probe; a
// no-op double-check inside the lock keeps the publish path idempotent if
// another writer evicted concurrently.
func (r *IAMResolver) evictLocked(bucket string) {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	old := r.cache.Load()
	if _, hit := (*old)[bucket]; !hit {
		return // another writer already evicted; skip the clone
	}
	next := cloneCacheWithout(old, bucket)
	r.cache.Store(next)
}

// cloneCache returns a new map with the same entries as src, sized for an
// upcoming insert.
func cloneCache(src *map[string]*resolverEntry, sizeHint int) map[string]*resolverEntry {
	dst := make(map[string]*resolverEntry, sizeHint)
	for k, v := range *src {
		dst[k] = v
	}
	return dst
}

// cloneCacheWithout returns a new map with the same entries as src, minus
// the named bucket.
func cloneCacheWithout(src *map[string]*resolverEntry, bucket string) *map[string]*resolverEntry {
	dst := make(map[string]*resolverEntry, len(*src))
	for k, v := range *src {
		if k == bucket {
			continue
		}
		dst[k] = v
	}
	return &dst
}
