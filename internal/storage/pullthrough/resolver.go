package pullthrough

import (
	"sync"

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
// (endpoint, access_key) tuple changes (rotation or reconfiguration).
//
// Cache invalidation is lazy: every Resolve compares the cached tuple to
// the live IAM record. No FSM event subscription is needed — callers see
// the new client at most one request after the FSM apply.
//
// Per /plan-eng-review override A8 — mu is sync.RWMutex. Cache-hit fast path
// takes RLock (no contention with other readers). Cache miss / rotation /
// invalidation takes Lock. Aligns with project memory feedback_lockfree_over_mutex.
type IAMResolver struct {
	store *iam.Store
	mu    sync.RWMutex
	cache map[string]*resolverEntry // bucket → cached client
}

type resolverEntry struct {
	endpoint  string
	accessKey string
	upstream  *S3Upstream
}

// NewIAMResolver returns a Resolver backed by the given IAM Store. The store
// must be non-nil; nil panics on first Resolve. Callers should fail-fast in
// the construction path rather than relying on this guard (see runtime
// wiring in serveruntime/run.go).
func NewIAMResolver(store *iam.Store) *IAMResolver {
	return &IAMResolver{
		store: store,
		cache: make(map[string]*resolverEntry),
	}
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
		// No live record — drop any stale cache entry and signal miss.
		r.mu.Lock()
		delete(r.cache, bucket)
		r.mu.Unlock()
		return nil, false
	}

	// Fast path: cache hit with matching (endpoint, access_key).
	r.mu.RLock()
	cached, hit := r.cache[bucket]
	if hit && cached.endpoint == rec.Endpoint && cached.accessKey == rec.AccessKey {
		up := cached.upstream
		r.mu.RUnlock()
		return up, true
	}
	r.mu.RUnlock()

	// Slow path: cache miss or rotation — build a new client under exclusive lock.
	r.mu.Lock()
	defer r.mu.Unlock()

	// Re-check after upgrading to write lock (another goroutine may have built it).
	cached, hit = r.cache[bucket]
	if hit && cached.endpoint == rec.Endpoint && cached.accessKey == rec.AccessKey {
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
		delete(r.cache, bucket)
		return nil, false
	}
	r.cache[bucket] = &resolverEntry{
		endpoint:  rec.Endpoint,
		accessKey: rec.AccessKey,
		upstream:  up,
	}
	return up, true
}
