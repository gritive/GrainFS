package server

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/s3auth"
)

func (s *Server) authorizePreLoad(ctx context.Context, c *app.RequestContext, bucket, key string, action s3auth.S3Action) bool {
	return s.authorizePreLoadWithDenyMessage(ctx, c, bucket, key, action, "")
}

func (s *Server) authorizePreLoadWithDenyMessage(ctx context.Context, c *app.RequestContext, bucket, key string, action s3auth.S3Action, denyMessage string) bool {
	decision := s.authz.Decide(ctx, authzInput(ctx, bucket, key, action, 0), s3auth.PhasePreLoad)
	rememberAuthzDecision(c, decision)
	if decision.Allow {
		return true
	}

	c.Set(auditErrReasonKey, decision.Reason)
	writeXMLError(c, consts.StatusForbidden, "AccessDenied", authzDenyMessage(decision.Reason, denyMessage))
	c.Abort()
	return false
}

func (s *Server) authorizePostLoad(ctx context.Context, c *app.RequestContext, bucket, key string, action s3auth.S3Action, aclByte uint8) bool {
	decision := s.authz.Decide(ctx, authzInput(ctx, bucket, key, action, aclByte), s3auth.PhasePostLoad)
	rememberAuthzDecision(c, decision)
	return decision.Allow
}

// rememberAuthzDecision stashes the Layer 1 decision metadata onto the request
// context so the audit envelope finalizer can populate matched_policy_id /
// matched_sid / authz_latency_us / condition_context_json on the audit.s3
// row without re-evaluating the policy. T51' §6.
func rememberAuthzDecision(c *app.RequestContext, decision s3auth.Decision) {
	if c == nil {
		return
	}
	c.Set(auditAuthzDecisionKey, decision)
	if decision.Allow && decision.Detail.AnonAllow {
		c.Set(auditAuthzAnonAllowKey, true)
	}
}

func authzInput(ctx context.Context, bucket, key string, action s3auth.S3Action, aclByte uint8) s3auth.PermCheckInput {
	return s3auth.PermCheckInput{
		Principal: s3auth.Principal{AccessKey: AccessKeyFromContext(ctx)},
		Resource:  s3auth.ResourceRef{Bucket: bucket, Key: key},
		Action:    action,
		ObjectACL: s3auth.ACLGrant(aclByte),
	}
}

func authzDenyMessage(reason, override string) string {
	if override != "" {
		return override
	}
	switch reason {
	case "no_grant":
		return "IAM grant denies this action"
	case "policy_deny":
		return "bucket policy denies this action"
	default:
		return "AccessDenied"
	}
}
