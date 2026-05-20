package server

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/s3auth"
)

func (s *Server) authorizeAccessKeyScope(ctx context.Context, c *app.RequestContext, bucket, key string, action s3auth.S3Action) bool {
	if !s.accessKeyScopeEnforced() {
		return true
	}
	scope := iam.ScopeFromContext(ctx)
	if iam.ScopeAllows(scope, bucket) {
		return true
	}

	saID := iam.PrincipalFromContext(ctx)
	s.iamAudit.RecordDeny(ctx, saID, bucket, key, action, "key_scope_mismatch")
	c.Set(auditErrReasonKey, "key_scope_mismatch")
	// Stash a minimal Decision so the audit envelope finalizer records the
	// reason on the audit.s3 row. matched_policy_id / matched_sid / latency /
	// condition_context stay empty — Layer 0 scope check ran before Layer 1.
	rememberAuthzDecision(c, s3auth.Decision{
		Allow:  false,
		Detail: s3auth.AuthzDetail{Reason: "key_scope_mismatch"},
	})
	writeXMLError(c, consts.StatusForbidden, "AccessDenied", "Access key scope denies this bucket")
	c.Abort()
	return false
}
