package server

import (
	"context"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/s3auth"
)

// s3ActionEnum maps an HTTP method + path context to an S3Action enum value.
// path is required to distinguish sub-resource operations (e.g., ?delete).
// hasPolicyQuery distinguishes bucket-policy CRUD (?policy) from regular
// bucket operations so authz can require Admin for Put/Delete and Read
// for Get on the policy resource.
func s3ActionEnum(method, path string, hasKey, hasPolicyQuery bool) s3auth.S3Action {
	if hasPolicyQuery && !hasKey {
		switch method {
		case "GET":
			return s3auth.GetBucketPolicy
		case "PUT":
			return s3auth.PutBucketPolicy
		case "DELETE":
			return s3auth.DeleteBucketPolicy
		}
	}
	switch method {
	case "GET":
		if hasKey {
			return s3auth.GetObject
		}
		return s3auth.ListBucket
	case "HEAD":
		if hasKey {
			return s3auth.HeadObject
		}
		return s3auth.ListBucket
	case "PUT":
		if hasKey {
			return s3auth.PutObject
		}
		return s3auth.CreateBucket
	case "DELETE":
		if hasKey {
			return s3auth.DeleteObject
		}
		return s3auth.DeleteBucket
	case "POST":
		return s3auth.PutObject // multipart upload
	default:
		return s3auth.UnknownAction
	}
}

// authzMiddleware checks IAM grants and bucket policies for authorized
// access. Must run after authMiddleware (which sets the access key and
// IAM principal in context).
//
// Two layers, evaluated in order:
//  1. IAM grant (only when iamStore.AuthEnabled() is sticky-on). Looks up
//     (sa_id, bucket) → role and the role's permission for the requested
//     S3 action. Deny → 403, audited as deny+no_grant.
//  2. Bucket policy (always, as second layer). Existing s.policyStore.Allow
//     can further restrict what IAM permits but cannot loosen it. Deny → 403,
//     audited as deny+policy_deny.
//
// Allow → audit allow, then forward.
//
// In anonymous mode (iamStore.AuthEnabled() == false), the IAM layer is
// skipped and only the bucket policy applies — this preserves the v0.0.92
// behavior for clusters that haven't enabled IAM.
func (s *Server) authzMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		path := string(c.URI().Path())

		// Skip non-S3 paths
		if path == "/" || path == "/metrics" || strings.HasPrefix(path, "/ui/") ||
			strings.HasPrefix(path, "/iceberg/") || strings.HasPrefix(path, "/admin/") {
			c.Next(ctx)
			return
		}

		bucket := c.Param("bucket")
		key := strings.TrimPrefix(c.Param("key"), "/")

		if bucket == "" {
			c.Next(ctx)
			return
		}

		// Phase 5d #4: ?policy CRUD now flows through the same 2-layer
		// authz chain as object/bucket ops. Pre-fix this skipped both
		// layers — any signed SA could read/write/delete any bucket's
		// policy. s3ActionEnum maps ?policy GET/PUT/DELETE to dedicated
		// S3Action values (GetBucketPolicy/PutBucketPolicy/DeleteBucketPolicy);
		// RoleAllows requires Read+ for GET and Admin for PUT/DELETE.
		hasPolicy := c.QueryArgs().Has("policy")
		action := s3ActionEnum(string(c.Method()), path, key != "", hasPolicy)
		accessKey := AccessKeyFromContext(ctx)

		// Layer 0: AccessKey bucket scope (only when auth is enabled).
		// scope = key.BucketScope. nil/empty → unrestricted (legacy keys,
		// backward compat). non-empty → bucket must be a member.
		if s.iamStore != nil && s.iamStore.AuthEnabled() {
			scope := iam.ScopeFromContext(ctx)
			if !iam.ScopeAllows(scope, bucket) {
				saID := iam.PrincipalFromContext(ctx)
				s.iamAudit.RecordDeny(ctx, saID, bucket, key, action, "key_scope_mismatch")
				writeXMLError(c, consts.StatusForbidden, "AccessDenied", "Access key scope denies this bucket")
				c.Abort()
				return
			}
		}

		// Layer 1: IAM grant (only when auth is enabled).
		if s.iamStore != nil && s.iamStore.AuthEnabled() {
			saID := iam.PrincipalFromContext(ctx)
			if !iam.CheckAccess(s.iamStore, saID, bucket, action) {
				s.iamAudit.RecordDeny(ctx, saID, bucket, key, action, "no_grant")
				writeXMLError(c, consts.StatusForbidden, "AccessDenied", "IAM grant denies this action")
				c.Abort()
				return
			}
		}

		// Layer 2: bucket policy (always applies, except for bucket-policy
		// CRUD itself — chicken-and-egg: a deny-all policy must remain
		// removable by the IAM-Admin SA without policy rules having to
		// allow s3:GetBucketPolicy/s3:PutBucketPolicy/s3:DeleteBucketPolicy
		// explicitly. IAM (Layer 1) already gates these operations.
		if action != s3auth.GetBucketPolicy && action != s3auth.PutBucketPolicy && action != s3auth.DeleteBucketPolicy {
			in := s3auth.PermCheckInput{
				Principal: s3auth.Principal{AccessKey: accessKey},
				Resource:  s3auth.ResourceRef{Bucket: bucket, Key: key},
				Action:    action,
			}
			if !s.policyStore.Allow(ctx, in) {
				saID := iam.PrincipalFromContext(ctx)
				s.iamAudit.RecordDeny(ctx, saID, bucket, key, action, "policy_deny")
				writeXMLError(c, consts.StatusForbidden, "AccessDenied", "bucket policy denies this action")
				c.Abort()
				return
			}
		}

		// Allow.
		if s.iamStore != nil && s.iamStore.AuthEnabled() {
			s.iamAudit.RecordAllow(ctx, iam.PrincipalFromContext(ctx), bucket, key, action)
		}
		c.Next(ctx)
	}
}
