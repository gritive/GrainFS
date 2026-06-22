package policy

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gritive/GrainFS/internal/s3auth"
)

// actionStrings maps S3Action enum to its string representation.
// Used at policy compile time to index rules by action.
var actionStrings = map[string]s3auth.S3Action{
	"s3:GetObject":                        s3auth.GetObject,
	"s3:HeadObject":                       s3auth.HeadObject,
	"s3:ListBucket":                       s3auth.ListBucket,
	"s3:PutObject":                        s3auth.PutObject,
	"s3:CreateBucket":                     s3auth.CreateBucket,
	"s3:DeleteObject":                     s3auth.DeleteObject,
	"s3:DeleteBucket":                     s3auth.DeleteBucket,
	"s3:CopyObject":                       s3auth.CopyObject,
	"s3:ListMultipartUploads":             s3auth.ListMultipartUploads,
	"s3:GetBucketPolicy":                  s3auth.GetBucketPolicy,
	"s3:PutBucketPolicy":                  s3auth.PutBucketPolicy,
	"s3:DeleteBucketPolicy":               s3auth.DeleteBucketPolicy,
	"s3:GetBucketVersioning":              s3auth.GetBucketVersioning,
	"s3:PutBucketVersioning":              s3auth.PutBucketVersioning,
	"s3:ListBucketVersions":               s3auth.ListBucketVersions,
	"s3:GetObjectRetention":               s3auth.GetObjectRetention,
	"s3:PutObjectRetention":               s3auth.PutObjectRetention,
	"s3:GetBucketObjectLockConfiguration": s3auth.GetBucketObjectLockConfiguration,
}

// actionAliases lists additional S3Action values covered by an action at compile time.
// AWS S3 compat: s3:GetObject authorizes both GET and HEAD on objects.
var actionAliases = map[s3auth.S3Action][]s3auth.S3Action{
	s3auth.GetObject: {s3auth.HeadObject},
}

type policyEffect bool

const (
	effectAllow policyEffect = true
	effectDeny  policyEffect = false
)

type principalMatcher interface {
	matches(accessKey string) bool
}

type anyPrincipal struct{}

func (anyPrincipal) matches(_ string) bool { return true }

type exactPrincipal struct{ key string }

func (e exactPrincipal) matches(accessKey string) bool { return e.key == accessKey }

type resourceMatcher interface {
	matches(bucket, key string) bool
}

// arnResource parses an S3 ARN and compares (bucket, key) directly without
// building an ARN string at match time — eliminating hot-path allocations.
type arnResource struct {
	bucket     string
	key        string // empty = bucket-level only
	prefixMode bool   // true when original ARN ends with "/*" (match any key)
}

// parseARN converts an S3 ARN to an arnResource.
//
//	"arn:aws:s3:::bucket"    → {bucket:"bucket", key:"",  prefixMode:false}
//	"arn:aws:s3:::bucket/*"  → {bucket:"bucket", key:"",  prefixMode:true}
//	"arn:aws:s3:::bucket/k"  → {bucket:"bucket", key:"k", prefixMode:false}
func parseARN(arn string) arnResource {
	const prefix = "arn:aws:s3:::"
	rest := strings.TrimPrefix(arn, prefix)

	if strings.HasSuffix(rest, "/*") {
		bucket := strings.TrimSuffix(rest, "/*")
		return arnResource{bucket: bucket, prefixMode: true}
	}

	if idx := strings.IndexByte(rest, '/'); idx >= 0 {
		return arnResource{bucket: rest[:idx], key: rest[idx+1:]}
	}
	return arnResource{bucket: rest}
}

func (r arnResource) matches(bucket, key string) bool {
	if r.bucket != bucket {
		return false
	}
	if r.prefixMode {
		return true // any key in this bucket
	}
	return r.key == key
}

type compiledRule struct {
	effect    policyEffect
	principal principalMatcher
	resource  resourceMatcher
}

func (r compiledRule) matches(in s3auth.PermCheckInput) bool {
	return r.principal.matches(in.Principal.AccessKey) &&
		r.resource.matches(in.Resource.Bucket, in.Resource.Key)
}

const maxS3Action = 32 // S3Action enum fits comfortably

type compiledPolicy struct {
	// rules[action] contains deny rules first, then allow rules.
	// "any deny wins" is enforced by iterating deny slice before allow slice.
	deny  [maxS3Action][]compiledRule
	allow [maxS3Action][]compiledRule
}

// policyState is the immutable snapshot published via atomic.Pointer.
// Set / Delete build a new state with cloned maps and publish atomically;
// Allow / GetRaw load the current snapshot without locking.
type policyState struct {
	byBucket map[string]*compiledPolicy
	raw      map[string][]byte
	negative map[string]struct{} // buckets resolved to "no enforceable policy" (allow)
}

// LoaderFunc loads a bucket's committed policy from the local replica for
// pull-on-miss. found=true → compile policyJSON. found=false,err=nil → no policy
// (allow, negative-cache). err!=nil → indeterminate read (allow, do not cache).
type LoaderFunc func(bucket string) (policyJSON []byte, found bool, err error)

// CompiledPolicyStore is a policy store that pre-compiles policies at Set() time
// for O(1) action lookup and deny-first evaluation.
// Implements s3auth.PolicyChecker.
//
// Reads (Allow, GetRaw) are lock-free: they load an immutable *policyState
// via atomic.Pointer and consult its maps directly. Writes (Set, Delete) are
// serialized by writeMu so two concurrent mutators produce a deterministic
// merge of their changes; they clone the current state, apply the mutation,
// and publish a new pointer. Policy updates are rare (bucket policy admin
// only) while Allow is called on every S3 request, so read-mostly CoW is
// the right trade-off.
//
// Audit follow-up: docs/architecture/lock-free-audit.md →
// "internal/policy/compiled.go - compiled policy map; request evaluation
// uses short read locks." Every S3 request takes that RLock; the atomic
// publication eliminates the read-side acquire/release overhead.
type CompiledPolicyStore struct {
	writeMu sync.Mutex
	state   atomic.Pointer[policyState]
	loader  atomic.Pointer[LoaderFunc]
	gen     atomic.Uint64 // bumped by Set/Delete/Invalidate; guards in-flight pull caching
}

// NewCompiledPolicyStore creates an empty store.
func NewCompiledPolicyStore() *CompiledPolicyStore {
	cs := &CompiledPolicyStore{}
	cs.state.Store(&policyState{
		byBucket: make(map[string]*compiledPolicy),
		raw:      make(map[string][]byte),
		negative: make(map[string]struct{}),
	})
	return cs
}

// SetLoader installs the pull-on-miss loader. Wire once at construction time.
func (cs *CompiledPolicyStore) SetLoader(fn LoaderFunc) { cs.loader.Store(&fn) }

// cloneState deep-copies the three maps for copy-on-write publication.
func cloneState(old *policyState) *policyState {
	next := &policyState{
		byBucket: make(map[string]*compiledPolicy, len(old.byBucket)+1),
		raw:      make(map[string][]byte, len(old.raw)+1),
		negative: make(map[string]struct{}, len(old.negative)+1),
	}
	for k, v := range old.byBucket {
		next.byBucket[k] = v
	}
	for k, v := range old.raw {
		next.raw[k] = v
	}
	for k := range old.negative {
		next.negative[k] = struct{}{}
	}
	return next
}

// compilePolicy parses and compiles a policy document into the action-indexed,
// deny-first representation. Extracted so both Set (write path) and pull-on-miss
// share one compile path.
func compilePolicy(policyJSON []byte) (*compiledPolicy, error) {
	bp, err := ParsePolicy(policyJSON)
	if err != nil {
		return nil, err
	}

	cp := &compiledPolicy{}
	for _, stmt := range bp.Statement {
		effect := effectAllow
		if stmt.Effect == "Deny" {
			effect = effectDeny
		}

		for _, p := range stmt.Principal.AWS {
			var pm principalMatcher
			if p == "*" {
				pm = anyPrincipal{}
			} else {
				pm = exactPrincipal{key: p}
			}

			for _, res := range stmt.Resource {
				rm := parseARN(res)

				rule := compiledRule{effect: effect, principal: pm, resource: rm}

				for _, actionStr := range stmt.Action {
					for action, enumVal := range actionStrings {
						if actionStr == action || (strings.HasSuffix(actionStr, ":*") &&
							strings.HasPrefix(action, strings.TrimSuffix(actionStr, "*"))) {
							targets := append([]s3auth.S3Action{enumVal}, actionAliases[enumVal]...)
							for _, target := range targets {
								if effect == effectDeny {
									cp.deny[target] = append(cp.deny[target], rule)
								} else {
									cp.allow[target] = append(cp.allow[target], rule)
								}
							}
						}
					}
				}
			}
		}
	}
	return cp, nil
}

// Set compiles and stores a policy for a bucket.
func (cs *CompiledPolicyStore) Set(bucket string, policyJSON []byte) error {
	cp, err := compilePolicy(policyJSON)
	if err != nil {
		return err
	}

	cs.writeMu.Lock()
	defer cs.writeMu.Unlock()
	cs.gen.Add(1) // publishing newer truth — invalidate any in-flight pull
	next := cloneState(cs.state.Load())
	next.byBucket[bucket] = cp
	next.raw[bucket] = append([]byte(nil), policyJSON...)
	delete(next.negative, bucket)
	cs.state.Store(next)
	return nil
}

// GetRaw returns the raw JSON policy for a bucket, or nil.
func (cs *CompiledPolicyStore) GetRaw(bucket string) []byte {
	raw := cs.state.Load().raw[bucket]
	if raw == nil {
		return nil
	}
	return append([]byte(nil), raw...)
}

// Delete removes the policy for a bucket.
func (cs *CompiledPolicyStore) Delete(bucket string) {
	cs.writeMu.Lock()
	defer cs.writeMu.Unlock()
	cs.gen.Add(1) // removing a policy — invalidate any in-flight pull so it can't re-cache it
	old := cs.state.Load()
	_, hadCompiled := old.byBucket[bucket]
	_, hadRaw := old.raw[bucket]
	_, hadNeg := old.negative[bucket]
	if !hadCompiled && !hadRaw && !hadNeg {
		return // no-op: avoid an unnecessary clone (gen already bumped above)
	}
	next := cloneState(old)
	delete(next.byBucket, bucket)
	delete(next.raw, bucket)
	delete(next.negative, bucket)
	cs.state.Store(next)
}

// Allow evaluates the permission check against compiled policy rules.
//
// Contract:
// 1. No policy for bucket → true (default allow, preserves existing behavior)
// 2. Policy exists + any deny rule matches → false ("any deny wins", AWS S3 compatible)
// 3. Policy exists + no deny match + any allow match → true
// 4. Policy exists + no deny match + no allow match → false (default deny)
func (cs *CompiledPolicyStore) Allow(_ context.Context, in s3auth.PermCheckInput) bool {
	if int(in.Action) >= maxS3Action {
		return false
	}
	st := cs.state.Load()
	bucket := in.Resource.Bucket
	if cp := st.byBucket[bucket]; cp != nil {
		return evaluate(cp, in)
	}
	if _, neg := st.negative[bucket]; neg {
		return true // resolved: no enforceable policy
	}
	return cs.pullAndAllow(in)
}

// evaluate applies deny-first / allow / default-deny against a compiled policy.
func evaluate(cp *compiledPolicy, in s3auth.PermCheckInput) bool {
	// Deny-first: any explicit deny wins
	for _, rule := range cp.deny[in.Action] {
		if rule.matches(in) {
			return false
		}
	}
	// Then check allow rules
	for _, rule := range cp.allow[in.Action] {
		if rule.matches(in) {
			return true
		}
	}
	return false // default deny when policy exists
}

// pullAndAllow resolves an unresolved bucket from the committed replica via the
// loader, caches the result (guarded by gen so a concurrent Set/Delete/Invalidate
// wins), and evaluates. See LoaderFunc for the outcome mapping.
func (cs *CompiledPolicyStore) pullAndAllow(in s3auth.PermCheckInput) bool {
	lp := cs.loader.Load()
	if lp == nil {
		return true // legacy: no loader wired
	}
	bucket := in.Resource.Bucket
	gen := cs.gen.Load() // capture BEFORE the read; Set/Delete/Invalidate bump it
	data, found, err := (*lp)(bucket)
	if err != nil {
		return true // indeterminate read → allow, NOT cached (retries, self-heals)
	}
	if !found {
		cs.cacheNegative(bucket, gen)
		return true
	}
	cp, perr := compilePolicy(data)
	if perr != nil {
		// Malformed committed policy → fail closed (empty compiledPolicy is
		// default-deny) and cache to avoid re-reading every request.
		cs.cacheCompiled(bucket, data, &compiledPolicy{}, gen)
		return false
	}
	cs.cacheCompiled(bucket, data, cp, gen)
	return evaluate(cp, in)
}

// cacheNegative records "no policy" for bucket, unless a concurrent mutation
// (gen bump) raced this pull — in which case the pulled view is stale and dropped.
func (cs *CompiledPolicyStore) cacheNegative(bucket string, gen uint64) {
	cs.writeMu.Lock()
	defer cs.writeMu.Unlock()
	if cs.gen.Load() != gen {
		return
	}
	old := cs.state.Load()
	if _, ok := old.negative[bucket]; ok {
		return
	}
	next := cloneState(old)
	next.negative[bucket] = struct{}{}
	cs.state.Store(next)
}

// cacheCompiled records a compiled policy for bucket, unless a concurrent
// mutation raced this pull (gen mismatch → drop the stale result).
func (cs *CompiledPolicyStore) cacheCompiled(bucket string, raw []byte, cp *compiledPolicy, gen uint64) {
	cs.writeMu.Lock()
	defer cs.writeMu.Unlock()
	if cs.gen.Load() != gen {
		return
	}
	next := cloneState(cs.state.Load())
	next.byBucket[bucket] = cp
	next.raw[bucket] = append([]byte(nil), raw...)
	delete(next.negative, bucket)
	cs.state.Store(next)
}
