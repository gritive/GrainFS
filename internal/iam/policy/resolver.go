package policy

import (
	"context"
	"strings"
	"sync"
	"time"
)

// Store is the data access contract the Resolver depends on.
// Implementations are expected to be provided by the IAM metadata layer.
type Store interface {
	SAPolicies(ctx context.Context, saID string) ([]string, error)
	SAGroups(ctx context.Context, saID string) ([]string, error)
	GroupPolicies(ctx context.Context, group string) ([]string, error)
	PolicyDoc(ctx context.Context, name string) (*Document, error)
	BucketPolicy(ctx context.Context, bucket string) (*Document, error)
}

type cacheEntry struct {
	expires time.Time
	pp      []*Document
	ppNames []string
	bp      *Document
}

// Resolver materializes the effective policy set for a (saID, bucket) pair
// and caches parsed results with a configurable TTL.
type Resolver struct {
	mu    sync.Mutex
	ttl   time.Duration
	store Store
	cache map[string]cacheEntry // key = saID + "|" + bucket
}

// NewResolver creates a Resolver backed by store with the given TTL.
func NewResolver(store Store, ttl time.Duration) *Resolver {
	return &Resolver{ttl: ttl, store: store, cache: make(map[string]cacheEntry)}
}

func cacheKey(sa, bucket string) string { return sa + "|" + bucket }

// Effective returns the union of principal-attached and bucket policies for
// the given (saID, bucket), using the cache when the entry is still fresh.
func (r *Resolver) Effective(ctx context.Context, saID, bucket string) (EvalInput, error) {
	k := cacheKey(saID, bucket)
	r.mu.Lock()
	if e, ok := r.cache[k]; ok && time.Now().Before(e.expires) {
		r.mu.Unlock()
		return EvalInput{
			PrincipalPolicies:    e.pp,
			PrincipalPolicyNames: e.ppNames,
			ResourcePolicy:       e.bp,
			ResourcePolicyBucket: bucket,
			Principal:            saID,
		}, nil
	}
	r.mu.Unlock()

	names, err := r.store.SAPolicies(ctx, saID)
	if err != nil {
		return EvalInput{}, err
	}
	groups, err := r.store.SAGroups(ctx, saID)
	if err != nil {
		return EvalInput{}, err
	}
	for _, g := range groups {
		gp, err := r.store.GroupPolicies(ctx, g)
		if err != nil {
			return EvalInput{}, err
		}
		names = append(names, gp...)
	}
	var pp []*Document
	var ppNames []string
	for _, n := range names {
		d, err := r.store.PolicyDoc(ctx, n)
		if err != nil {
			return EvalInput{}, err
		}
		if d != nil {
			pp = append(pp, d)
			ppNames = append(ppNames, n)
		}
	}
	bp, err := r.store.BucketPolicy(ctx, bucket)
	if err != nil {
		return EvalInput{}, err
	}

	r.mu.Lock()
	r.cache[k] = cacheEntry{expires: time.Now().Add(r.ttl), pp: pp, ppNames: ppNames, bp: bp}
	r.mu.Unlock()
	return EvalInput{
		PrincipalPolicies:    pp,
		PrincipalPolicyNames: ppNames,
		ResourcePolicy:       bp,
		ResourcePolicyBucket: bucket,
		Principal:            saID,
	}, nil
}

// HasBucketPolicy reports whether an explicit bucket policy exists for bucket.
// Implicit policies (e.g. the "default" bucket's anon Allow per spec D#2) are
// NOT counted — only explicit operator-attached policies via BucketPolicyPut.
//
// Returns (false, nil) if the underlying store reports not-found.
func (r *Resolver) HasBucketPolicy(ctx context.Context, bucket string) (bool, error) {
	d, err := r.store.BucketPolicy(ctx, bucket)
	if err != nil {
		return false, err
	}
	return d != nil, nil
}

// Invalidate removes cache entries matching any of the given SA IDs or bucket names.
// Passing empty slices for both arguments nukes the entire cache (global mutation path,
// e.g. a policy document body edit that affects unknown consumers).
func (r *Resolver) Invalidate(saIDs, buckets []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(saIDs) == 0 && len(buckets) == 0 {
		r.cache = make(map[string]cacheEntry)
		return
	}
	saSet := make(map[string]bool, len(saIDs))
	for _, s := range saIDs {
		saSet[s] = true
	}
	buSet := make(map[string]bool, len(buckets))
	for _, b := range buckets {
		buSet[b] = true
	}
	for k := range r.cache {
		i := strings.IndexByte(k, '|')
		if i < 0 {
			continue
		}
		sa, bu := k[:i], k[i+1:]
		if saSet[sa] || buSet[bu] {
			delete(r.cache, k)
		}
	}
}
