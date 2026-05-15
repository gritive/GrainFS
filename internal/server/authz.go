package server

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/s3auth"
)

// authzMiddleware checks IAM grants and bucket policies for authorized access.
// Must run after authMiddleware (which sets the access key and IAM principal
// in context).
//
// Layer 0 (AccessKey bucket scope) is owned by authorizeAccessKeyScope. Layers
// 1-3 (IAM grant, bucket policy, object ACL) are owned by
// `s3auth.RequestAuthorizer.Decide`; this middleware invokes the authorizer at
// PhasePreLoad and handlers re-invoke at PhasePostLoad after loading the
// target object's ACL.
//
// Production wires IAM unconditionally (v0.0.110.0+ — sticky `auth_enabled`
// bit removed). The iamStore!=nil guard remains so unit tests targeting
// non-auth concerns can still drive the server with no IAM wiring.
func (s *Server) authzMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		path := string(c.URI().Path())

		if routeSkipsS3Authz(path) {
			c.Next(ctx)
			return
		}

		if isBucketFormPost(c) {
			c.Next(ctx)
			return
		}

		req := s3AuthzRequestFromHertz(c)
		if req.Bucket == "" {
			c.Next(ctx)
			return
		}

		// Internal audit artifacts are read-only through S3 so Iceberg/DuckDB can
		// fetch metadata and data files returned by the REST catalog. Mutations,
		// bucket-level reads, and listing remain blocked.
		switch s.authorizeAuditInternalBucket(ctx, c, req.Bucket, req.Key, req.Method, req.Action) {
		case auditInternalBucketAllowed:
			c.Next(ctx)
			return
		case auditInternalBucketDenied:
			return
		case auditInternalBucketContinueSigned:
			// Signed callers still pass through the normal IAM/policy/ACL checks
			// below so ad hoc DuckDB analysis can read Iceberg files.
		}

		if !s.authorizeAccessKeyScope(ctx, c, req.Bucket, req.Key, req.Action) {
			return
		}

		if !s.authorizePreLoad(ctx, c, req.Bucket, req.Key, req.Action) {
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
	if s.authorizePreLoadWithDenyMessage(ctx, c, bucket, key, action, "Access Denied") {
		return false
	}
	c.Set(auditErrReasonKey, "cross_bucket_deny")
	return true
}

// mustAuthorizePostLoad evaluates the request at PhasePostLoad with the target
// object's ACL. On deny it writes an AccessDenied XML response and reports
// denied=true so the caller can return immediately. Handlers call this after
// loading the object's metadata; the same request was already pre-load gated
// by authzMiddleware (or, for cross-bucket reads, by mustAuthorize).
func (s *Server) mustAuthorizePostLoad(ctx context.Context, c *app.RequestContext, bucket, key string, action s3auth.S3Action, aclByte uint8) (denied bool) {
	if auditInternalObjectReadAllowed(bucket, key, string(c.Method()), c.RemoteAddr().String(), AccessKeyFromContext(ctx), s.auditInternalAccessKey) {
		return false
	}
	if s.authorizePostLoad(ctx, bucket, key, action, aclByte) {
		return false
	}
	c.Set(auditErrReasonKey, "object_acl_deny")
	writeXMLError(c, consts.StatusForbidden, "AccessDenied", "Access Denied")
	return true
}
