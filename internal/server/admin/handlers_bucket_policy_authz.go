package admin

import (
	"context"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/iam/policy"
)

const (
	bucketPolicyActionRead   = "grainfs:BucketPolicyRead"
	bucketPolicyActionWrite  = "grainfs:BucketPolicyWrite"
	bucketPolicyActionDelete = "grainfs:BucketPolicyDelete"
)

func authorizeBucketPolicyActor(ctx context.Context, d *Deps, bucket, action string) *Error {
	actor, ok := ActorPrincipalFromContext(ctx)
	if !ok {
		return nil
	}
	resource := bucketPolicyResource(bucket)
	if d == nil || d.AdminAuthz == nil {
		logAdminAuthzDecision(actorLogFields(ctx), action, resource, policy.EvalResult{Decision: policy.DecisionDeny, Reason: "authorizer not configured"})
		return NewForbidden("bucket policy permission denied: authorizer not configured")
	}
	result := d.AdminAuthz.AuthorizePrincipal(ctx, actor, bucket, policy.RequestContext{
		Action:   action,
		Resource: resource,
	})
	logAdminAuthzDecision(actorLogFields(ctx), action, resource, result)
	if result.Decision == policy.DecisionAllow {
		return nil
	}
	msg := "bucket policy permission denied"
	if result.Reason != "" {
		msg += ": " + result.Reason
	}
	return NewForbidden(msg)
}

func bucketPolicyResource(bucket string) string {
	return "arn:aws:s3:::" + bucket
}

type adminActorLogFields struct {
	Kind string
	ID   string
}

func actorLogFields(ctx context.Context) adminActorLogFields {
	actor, ok := ActorPrincipalFromContext(ctx)
	if !ok {
		return adminActorLogFields{}
	}
	return adminActorLogFields{Kind: string(actor.Kind), ID: actor.ID}
}

func logAdminAuthzDecision(actor adminActorLogFields, action, resource string, result policy.EvalResult) {
	decision := "deny"
	if result.Decision == policy.DecisionAllow {
		decision = "allow"
	}
	log.Info().
		Str("event", "admin_authz").
		Str("principal_kind", actor.Kind).
		Str("principal_id", actor.ID).
		Str("action", action).
		Str("resource", resource).
		Str("decision", decision).
		Str("reason", result.Reason).
		Str("matched_policy_id", result.MatchedPolicy).
		Str("matched_sid", result.MatchedSid).
		Msg("admin.authz")
}
