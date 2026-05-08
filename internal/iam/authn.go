package iam

import (
	"context"
)

// SecretLookup is the function type used by s3auth.Verifier to resolve
// access_key → secret_key. IAM-backed implementations return ok=false for
// revoked or expired keys so that SigV4 verification fails closed.
type SecretLookup func(accessKey string) (secret string, ok bool)

// NewSecretLookup returns a SecretLookup closure that reads from the
// given IAM Store. Honors KeyStatus and ExpiresAt: revoked or expired
// keys produce ok=false.
func NewSecretLookup(s *Store) SecretLookup {
	return func(ak string) (string, bool) {
		k, ok := s.LookupKey(ak)
		if !ok {
			return "", false
		}
		return k.SecretKey, true
	}
}

// ResolveSA returns (key, sa_id, ok). The *AccessKey is the resolved key
// record (live snapshot pointer); callers can read BucketScope and other
// fields without a second LookupKey call. Returns ok=false for revoked or
// expired keys (callers must fail closed).
func ResolveSA(s *Store, accessKey string) (*AccessKey, string, bool) {
	k, ok := s.LookupKey(accessKey)
	if !ok {
		return nil, "", false
	}
	return k, k.SAID, true
}

// principalCtxKey is the unexported context key carrying the resolved sa_id.
// Distinct from server.AccessKeyFromContext (which carries the public
// access_key string) — both may live on the same request context.
type principalCtxKey struct{}

// WithPrincipal returns a new context with sa_id set as the request
// principal. Called by the auth middleware after ResolveSA succeeds.
func WithPrincipal(ctx context.Context, saID string) context.Context {
	return context.WithValue(ctx, principalCtxKey{}, saID)
}

// PrincipalFromContext returns the sa_id for the current request, or
// empty string if no principal was set (anonymous mode or pre-auth path).
func PrincipalFromContext(ctx context.Context) string {
	v, _ := ctx.Value(principalCtxKey{}).(string)
	return v
}

// scopeCtxKey is a separate ctx key for the AccessKey's BucketScope.
// Distinct from principalCtxKey to keep saID and scope independently
// readable; legacy callers of PrincipalFromContext keep working unchanged.
type scopeCtxKey struct{}

// WithPrincipalScope returns a new ctx with the AccessKey's bucket_scope
// attached. nil/empty means unrestricted (no Layer 0 filter applied).
func WithPrincipalScope(ctx context.Context, scope []string) context.Context {
	return context.WithValue(ctx, scopeCtxKey{}, scope)
}

// ScopeFromContext returns the bucket_scope of the resolved AccessKey,
// or nil if none was set (anonymous mode, pre-auth path, or legacy
// unrestricted key).
func ScopeFromContext(ctx context.Context) []string {
	v, _ := ctx.Value(scopeCtxKey{}).([]string)
	return v
}
