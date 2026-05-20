package server

import (
	"context"
	"net/http"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/s3auth"
)

type auditInternalBucketAuthzResult uint8

const (
	auditInternalBucketNotApplicable auditInternalBucketAuthzResult = iota
	auditInternalBucketAllowed
	auditInternalBucketContinueSigned
	auditInternalBucketDenied
)

func (s *Server) authorizeAuditInternalBucket(ctx context.Context, c *app.RequestContext, bucket, key, method string, action s3auth.S3Action) auditInternalBucketAuthzResult {
	if bucket != audit.BucketName {
		return auditInternalBucketNotApplicable
	}

	accessKey := AccessKeyFromContext(ctx)
	if auditInternalObjectReadAllowed(bucket, key, method, c.RemoteAddr().String(), accessKey, s.auditInternalAccessKey) {
		return auditInternalBucketAllowed
	}

	if s.signedAuditObjectRead(ctx, bucket, key, method) {
		return auditInternalBucketContinueSigned
	}

	s.iamAudit.RecordDeny(ctx, iam.PrincipalFromContext(ctx), bucket, key, action, "internal_bucket")
	// Deny on internal-bucket reservation. Sets auditErrReasonKey so the audit
	// envelope finalizer records the reason on the audit.s3 row. Policy-decision
	// columns (matched_policy_id / matched_sid / authz_latency_us /
	// condition_context_json) stay empty because no Layer 1 policy was evaluated
	// for this rejection.
	c.Set(auditErrReasonKey, "internal_bucket")
	writeXMLError(c, consts.StatusForbidden, "AccessDenied", "Access denied to internal bucket")
	c.Abort()
	return auditInternalBucketDenied
}

func (s *Server) authenticateAuditInternalRequest(ctx context.Context, c *app.RequestContext, r *http.Request, bucket, key, method string) (context.Context, bool) {
	if s.auditInternalVerifier != nil && auditInternalObjectRequest(bucket, key, method, c.RemoteAddr().String()) {
		if accessKey, err := s.auditInternalVerifier.Verify(r); err == nil && accessKey == s.auditInternalAccessKey {
			return WithAccessKey(ctx, accessKey), true
		}
	}
	if auditInternalObjectReadAllowed(bucket, key, method, c.RemoteAddr().String(), AccessKeyFromContext(ctx), s.auditInternalAccessKey) {
		return ctx, true
	}
	return ctx, false
}

func auditInternalObjectRequest(bucket, key, method, remoteAddr string) bool {
	return auditObjectReadRequest(bucket, key, method) && isLocalhostAddr(remoteAddr)
}

func auditObjectReadRequest(bucket, key, method string) bool {
	return bucket == audit.BucketName && key != "" && (method == "GET" || method == "HEAD")
}

func auditInternalObjectReadAllowed(bucket, key, method, remoteAddr, accessKey, internalAccessKey string) bool {
	return internalAccessKey != "" && accessKey == internalAccessKey && auditInternalObjectRequest(bucket, key, method, remoteAddr)
}
