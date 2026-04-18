package server

import (
	"context"
	"strings"
	"sync"

	"github.com/gritive/GrainFS/internal/s3auth"
)

// actionStrings maps S3Action enum to its string representation.
// Used at policy compile time to index rules by action.
var actionStrings = map[string]s3auth.S3Action{
	"s3:GetObject":    s3auth.GetObject,
	"s3:HeadObject":   s3auth.HeadObject,
	"s3:ListBucket":   s3auth.ListBucket,
	"s3:PutObject":    s3auth.PutObject,
	"s3:CreateBucket": s3auth.CreateBucket,
	"s3:DeleteObject": s3auth.DeleteObject,
	"s3:DeleteBucket": s3auth.DeleteBucket,
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

// CompiledPolicyStore is a policy store that pre-compiles policies at Set() time
// for O(1) action lookup and deny-first evaluation.
// Implements s3auth.Authorizer.
type CompiledPolicyStore struct {
	mu       sync.RWMutex
	byBucket map[string]*compiledPolicy
	raw      map[string][]byte
}

// NewCompiledPolicyStore creates an empty store.
func NewCompiledPolicyStore() *CompiledPolicyStore {
	return &CompiledPolicyStore{
		byBucket: make(map[string]*compiledPolicy),
		raw:      make(map[string][]byte),
	}
}

// Set compiles and stores a policy for a bucket.
func (cs *CompiledPolicyStore) Set(bucket string, policyJSON []byte) error {
	bp, err := ParsePolicy(policyJSON)
	if err != nil {
		return err
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

	cs.mu.Lock()
	cs.byBucket[bucket] = cp
	cs.raw[bucket] = policyJSON
	cs.mu.Unlock()
	return nil
}

// GetRaw returns the raw JSON policy for a bucket, or nil.
func (cs *CompiledPolicyStore) GetRaw(bucket string) []byte {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.raw[bucket]
}

// Delete removes the policy for a bucket.
func (cs *CompiledPolicyStore) Delete(bucket string) {
	cs.mu.Lock()
	delete(cs.byBucket, bucket)
	delete(cs.raw, bucket)
	cs.mu.Unlock()
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

	cs.mu.RLock()
	cp := cs.byBucket[in.Resource.Bucket]
	cs.mu.RUnlock()

	if cp == nil {
		return true // no policy = no restriction
	}

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
