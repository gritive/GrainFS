package admin

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/gritive/GrainFS/internal/iam/policy"
)

type adminRouteAuthzSpec struct {
	action   string
	resource func(*app.RequestContext) string
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
