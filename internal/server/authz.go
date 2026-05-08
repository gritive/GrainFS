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

// authzMiddleware checks IAM grants and bucket policies for authorized access.
// Must run after authMiddleware (which sets the access key and IAM principal
// in context).
//
// Layer 0 (AccessKey bucket scope) lives here because it depends on iam-specific
// request context (`iam.ScopeFromContext`). Layers 1-3 (IAM grant, bucket
// policy, object ACL) are owned by `s3auth.RequestAuthorizer.Decide`; this
// middleware invokes the authorizer at PhasePreLoad and handlers re-invoke at
// PhasePostLoad after loading the target object's ACL.
func (s *Server) authzMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		path := string(c.URI().Path())

		// Skip non-S3 paths.
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

		hasPolicy := c.QueryArgs().Has("policy")
		action := s3ActionEnum(string(c.Method()), path, key != "", hasPolicy)
		accessKey := AccessKeyFromContext(ctx)

		// Layer 0: AccessKey bucket scope (only when auth is enabled).
		// Bucket-scoped keys must have this bucket in their scope; nil/empty
		// scope means unrestricted (legacy keys, backward compat).
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

		// Layers 1-3: IAM grant + bucket policy + (post-load ACL).
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
			writeXMLError(c, consts.StatusForbidden, "AccessDenied", msg)
			c.Abort()
			return
		}

		c.Next(ctx)
	}
}
