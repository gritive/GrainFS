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
// Layers, evaluated in order:
//  0. AccessKey bucket scope (only when IAM is wired). scope =
//     key.BucketScope. nil/empty → unrestricted (legacy keys). non-empty
//     → bucket must be a member.
//  1. IAM grant (only when IAM is wired). Looks up (sa_id, bucket) →
//     role and the role's permission for the requested S3 action.
//     Deny → 403, audited as deny+no_grant.
//  2. Bucket policy (always). Existing s.policyStore.Allow can further
//     restrict what IAM permits but cannot loosen it. Deny → 403,
//     audited as deny+policy_deny.
//
// Allow → audit allow (when IAM is wired), then forward.
//
// Production wires IAM unconditionally (v0.0.107.0+). The iamStore!=nil
// guard exists so unit tests targeting non-auth concerns can still drive
// the server with no IAM wiring.
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

		saID := iam.PrincipalFromContext(ctx)

		// Layers 0 and 1 only run when IAM is wired (production: always).
		if s.iamStore != nil {
			// Layer 0: AccessKey bucket scope. scope = key.BucketScope.
			// nil/empty → unrestricted (legacy keys, backward compat).
			// non-empty → bucket must be a member.
			scope := iam.ScopeFromContext(ctx)
			if !iam.ScopeAllows(scope, bucket) {
				s.iamAudit.RecordDeny(ctx, saID, bucket, key, action, "key_scope_mismatch")
				writeXMLError(c, consts.StatusForbidden, "AccessDenied", "Access key scope denies this bucket")
				c.Abort()
				return
			}

			// Layer 1: IAM grant.
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
				s.iamAudit.RecordDeny(ctx, saID, bucket, key, action, "policy_deny")
				writeXMLError(c, consts.StatusForbidden, "AccessDenied", "bucket policy denies this action")
				c.Abort()
				return
			}
		}

		// Allow.
		if s.iamStore != nil {
			s.iamAudit.RecordAllow(ctx, saID, bucket, key, action)
		}
		c.Next(ctx)
	}
}
