package server

import (
	"context"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/audit"
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

// authzMiddleware checks IAM grants and bucket policies for authorized access.
// Must run after authMiddleware (which sets the access key and IAM principal
// in context).
//
// Layer 0 (AccessKey bucket scope) lives here because it depends on iam-specific
// request context (`iam.ScopeFromContext`). Layers 1-3 (IAM grant, bucket
// policy, object ACL) are owned by `s3auth.RequestAuthorizer.Decide`; this
// middleware invokes the authorizer at PhasePreLoad and handlers re-invoke at
// PhasePostLoad after loading the target object's ACL.
//
// Production wires IAM unconditionally (v0.0.110.0+ — sticky `auth_enabled`
// bit removed). The iamStore!=nil guard remains so unit tests targeting
// non-auth concerns can still drive the server with no IAM wiring.
func (s *Server) authzMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		path := string(c.URI().Path())

		// Skip non-S3 paths.
		if path == "/" || path == "/metrics" || strings.HasPrefix(path, "/ui/") ||
			strings.HasPrefix(path, "/iceberg/") || strings.HasPrefix(path, "/admin/") {
			c.Next(ctx)
			return
		}

		if isBucketFormPost(c) {
			c.Next(ctx)
			return
		}

		bucket := c.Param("bucket")
		key := strings.TrimPrefix(c.Param("key"), "/")

		if bucket == "" {
			c.Next(ctx)
			return
		}

		hasPolicy := c.QueryArgs().Has("policy")
		action := s3ActionEnum(string(c.Method()), path, key != "", hasPolicy)
		accessKey := AccessKeyFromContext(ctx)

		// Internal audit artifacts are read-only through S3 so Iceberg/DuckDB can
		// fetch metadata and data files returned by the REST catalog. Mutations,
		// bucket-level reads, and listing remain blocked.
		if bucket == audit.BucketName {
			method := string(c.Method())
			if key != "" && (method == "GET" || method == "HEAD") {
				c.Next(ctx)
				return
			}
			s.iamAudit.RecordDeny(ctx, iam.PrincipalFromContext(ctx), bucket, key, action, "internal_bucket")
			c.Set(auditErrReasonKey, "internal_bucket")
			writeXMLError(c, consts.StatusForbidden, "AccessDenied", "Access denied to internal bucket")
			c.Abort()
			return
		}

		// Layer 0: AccessKey bucket scope (always-on; previously gated on
		// the now-removed sticky `auth_enabled` bit). Bucket-scoped keys
		// must have this bucket in their scope; nil/empty scope means
		// unrestricted (legacy keys, backward compat). The iamStore!=nil
		// guard preserves test setups that don't wire IAM at all.
		if s.iamStore != nil {
			scope := iam.ScopeFromContext(ctx)
			if !iam.ScopeAllows(scope, bucket) {
				saID := iam.PrincipalFromContext(ctx)
				s.iamAudit.RecordDeny(ctx, saID, bucket, key, action, "key_scope_mismatch")
				c.Set(auditErrReasonKey, "key_scope_mismatch")
				writeXMLError(c, consts.StatusForbidden, "AccessDenied", "Access key scope denies this bucket")
				c.Abort()
				return
			}
		}

		// Layers 1-3: IAM grant + bucket policy + (post-load ACL) via
		// s3auth.RequestAuthorizer (PR #250).
		in := s3auth.PermCheckInput{
			Principal: s3auth.Principal{AccessKey: accessKey},
			Resource:  s3auth.ResourceRef{Bucket: bucket, Key: key},
			Action:    action,
		}
		decision := s.authz.Decide(ctx, in, s3auth.PhasePreLoad)
		if !decision.Allow {
			msg := "AccessDenied"
			switch decision.Reason {
			case "no_grant":
				msg = "IAM grant denies this action"
			case "policy_deny":
				msg = "bucket policy denies this action"
			}
			c.Set(auditErrReasonKey, decision.Reason)
			writeXMLError(c, consts.StatusForbidden, "AccessDenied", msg)
			c.Abort()
			return
		}

		c.Next(ctx)
	}
}

// mustAuthorize evaluates the request at PhasePreLoad. On deny it writes an
// AccessDenied XML response and reports denied=true so the caller can return
// immediately. Used by handlers that need a cross-bucket pre-load check (e.g.,
// CopyObject source bucket); the request's primary bucket is already gated by
// authzMiddleware before any handler runs.
func (s *Server) mustAuthorize(ctx context.Context, c *app.RequestContext, bucket, key string, action s3auth.S3Action) (denied bool) {
	in := s3auth.PermCheckInput{
		Principal: s3auth.Principal{AccessKey: AccessKeyFromContext(ctx)},
		Resource:  s3auth.ResourceRef{Bucket: bucket, Key: key},
		Action:    action,
	}
	if s.authz.Decide(ctx, in, s3auth.PhasePreLoad).Allow {
		return false
	}
	c.Set(auditErrReasonKey, "cross_bucket_deny")
	writeXMLError(c, consts.StatusForbidden, "AccessDenied", "Access Denied")
	return true
}

// mustAuthorizePostLoad evaluates the request at PhasePostLoad with the target
// object's ACL. On deny it writes an AccessDenied XML response and reports
// denied=true so the caller can return immediately. Handlers call this after
// loading the object's metadata; the same request was already pre-load gated
// by authzMiddleware (or, for cross-bucket reads, by mustAuthorize).
func (s *Server) mustAuthorizePostLoad(ctx context.Context, c *app.RequestContext, bucket, key string, action s3auth.S3Action, aclByte uint8) (denied bool) {
	in := s3auth.PermCheckInput{
		Principal: s3auth.Principal{AccessKey: AccessKeyFromContext(ctx)},
		Resource:  s3auth.ResourceRef{Bucket: bucket, Key: key},
		Action:    action,
		ObjectACL: s3auth.ACLGrant(aclByte),
	}
	if s.authz.Decide(ctx, in, s3auth.PhasePostLoad).Allow {
		return false
	}
	c.Set(auditErrReasonKey, "object_acl_deny")
	writeXMLError(c, consts.StatusForbidden, "AccessDenied", "Access Denied")
	return true
}
