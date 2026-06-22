package admin

import (
	"context"
	"encoding/json"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/iam/principal"
)

type adminRouteAuthzSpec struct {
	action   string
	resource func(*app.RequestContext) string
	guard    adminRouteMutationGuard
}

func adminRouteAuthzMiddleware(d *Deps, spec adminRouteAuthzSpec) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		actor, ok := ActorPrincipalFromContext(ctx)
		if !ok {
			c.Next(ctx)
			return
		}
		resource := spec.resource(c)
		if d == nil || d.AdminAuthz == nil {
			result := policy.EvalResult{Decision: policy.DecisionDeny, Reason: "authorizer not configured"}
			logAdminAuthzDecision(actorLogFields(ctx), spec.action, resource, result)
			writeError(c, NewForbidden("admin permission denied: authorizer not configured"))
			c.Abort()
			return
		}
		result := d.AdminAuthz.AuthorizePrincipal(ctx, actor, "", policy.RequestContext{
			Action:   spec.action,
			Resource: resource,
		})
		logAdminAuthzDecision(actorLogFields(ctx), spec.action, resource, result)
		if result.Decision == policy.DecisionAllow {
			if spec.guard != nil {
				if err := spec.guard(ctx, c, d, actor); err != nil {
					logAdminAuthzDecision(actorLogFields(ctx), spec.action, resource, policy.EvalResult{Decision: policy.DecisionDeny, Reason: err.Message})
					writeError(c, err)
					c.Abort()
					return
				}
			}
			c.Next(ctx)
			return
		}
		msg := "admin permission denied"
		if result.Reason != "" {
			msg += ": " + result.Reason
		}
		writeError(c, NewForbidden(msg))
		c.Abort()
	}
}

type adminRouteMutationGuard func(context.Context, *app.RequestContext, *Deps, principal.Principal) *Error

// AdminSelfEffectGuard is implemented by runtime adapters that can answer
// whether a bearer actor would change its own effective IAM policies.
type AdminSelfEffectGuard interface {
	PolicyAffectsPrincipal(ctx context.Context, actor principal.Principal, policyName string) (bool, error)
	GroupAffectsPrincipal(ctx context.Context, actor principal.Principal, group string) (bool, error)
}

func iamSAResource(c *app.RequestContext) string {
	if id := c.Param("id"); id != "" {
		return "iam/sa/" + id
	}
	return "iam/sa/*"
}

func iamPolicyResource(c *app.RequestContext) string {
	if name := c.Param("name"); name != "" {
		return "iam/policy/" + name
	}
	return "iam/policy/*"
}

func iamPolicyAttachSAResource(c *app.RequestContext) string {
	name := c.Param("name")
	said := c.Param("said")
	if name != "" && said != "" {
		return "iam/policy/" + name + "/attach/sa/" + said
	}
	return "iam/policy/*/attach/sa/*"
}

func iamGroupResource(c *app.RequestContext) string {
	if name := c.Param("name"); name != "" {
		return "iam/group/" + name
	}
	return "iam/group/*"
}

func iamGroupPolicyResource(c *app.RequestContext) string {
	name := c.Param("name")
	policyName := c.Param("policy")
	if name != "" && policyName != "" {
		return "iam/group/" + name + "/policy/" + policyName
	}
	return "iam/group/*/policy/*"
}

func iamBucketUpstreamResource(c *app.RequestContext) string {
	if bucket := c.Param("bucket"); bucket != "" {
		return "iam/upstream/" + bucket
	}
	var req iam.BucketUpstreamPutRequest
	if body := c.Request.Body(); len(body) > 0 && json.Unmarshal(body, &req) == nil && req.Bucket != "" {
		return "iam/upstream/" + req.Bucket
	}
	return "iam/upstream/*"
}

func iamBucketUpstreamCutoverResource(c *app.RequestContext) string {
	var req iam.BucketUpstreamCutoverRequest
	if body := c.Request.Body(); len(body) > 0 && json.Unmarshal(body, &req) == nil && req.Bucket != "" {
		return "iam/upstream/" + req.Bucket + "/cutover"
	}
	return "iam/upstream/*/cutover"
}

func adminConfigResource(c *app.RequestContext) string {
	if key := c.Param("key"); key != "" {
		return "admin/config/" + key
	}
	return "admin/config/*"
}

func adminDashboardTokenResource(_ *app.RequestContext) string {
	return "admin/dashboard/token"
}

func adminDashboardTokenRotateResource(_ *app.RequestContext) string {
	return "admin/dashboard/token/rotate"
}

func denyPolicyIfSelfEffective(ctx context.Context, c *app.RequestContext, d *Deps, actor principal.Principal) *Error {
	if d == nil {
		return NewForbidden("admin permission denied: self-effect guard not configured")
	}
	guard, ok := d.IAMPolicy.(AdminSelfEffectGuard)
	if !ok {
		return NewForbidden("admin permission denied: self-effect guard not configured")
	}
	affects, err := guard.PolicyAffectsPrincipal(ctx, actor, c.Param("name"))
	if err != nil {
		return NewInternal("check self-effective policy: " + err.Error())
	}
	if affects {
		return NewForbidden("admin permission denied: policy affects caller's effective policies")
	}
	return nil
}

func denyDirectSelfPolicyAttach(_ context.Context, c *app.RequestContext, _ *Deps, actor principal.Principal) *Error {
	if c.Param("said") == actor.ID {
		return NewForbidden("admin permission denied: cannot change caller's direct policy attachment")
	}
	return nil
}

func denyDirectSelfGroupMember(_ context.Context, c *app.RequestContext, _ *Deps, actor principal.Principal) *Error {
	if c.Param("said") == actor.ID {
		return NewForbidden("admin permission denied: cannot change caller's group membership")
	}
	return nil
}

func denyGroupIfSelfEffective(ctx context.Context, c *app.RequestContext, d *Deps, actor principal.Principal) *Error {
	if d == nil {
		return NewForbidden("admin permission denied: self-effect guard not configured")
	}
	guard, ok := d.IAMPolicy.(AdminSelfEffectGuard)
	if !ok {
		return NewForbidden("admin permission denied: self-effect guard not configured")
	}
	affects, err := guard.GroupAffectsPrincipal(ctx, actor, c.Param("name"))
	if err != nil {
		return NewInternal("check self-effective group: " + err.Error())
	}
	if affects {
		return NewForbidden("admin permission denied: group affects caller's effective policies")
	}
	return nil
}
