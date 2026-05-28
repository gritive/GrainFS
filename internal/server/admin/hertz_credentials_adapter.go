package admin

import (
	"context"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func credentialActorMiddleware(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		authHeader := string(c.Request.Header.Get("Authorization"))
		if authHeader == "" {
			c.Next(ctx)
			return
		}
		token, ok := parseBearerToken(authHeader)
		if !ok && !looksLikeBearer(authHeader) {
			c.Next(ctx)
			return
		}
		if !ok {
			writeError(c, NewUnauthorized("invalid bearer token"))
			c.Abort()
			return
		}
		if token == "" {
			writeError(c, NewUnauthorized("invalid bearer token"))
			c.Abort()
			return
		}
		if d == nil || d.ActorAuth == nil {
			writeError(c, NewUnauthorized("bearer authentication not configured"))
			c.Abort()
			return
		}
		actor, err := d.ActorAuth.AuthenticateActor(ctx, token)
		if err != nil {
			writeError(c, NewUnauthorized("invalid bearer token"))
			c.Abort()
			return
		}
		c.Next(WithActorPrincipal(ctx, actor))
	}
}

func parseBearerToken(s string) (string, bool) {
	trimmed := strings.TrimSpace(s)
	if len(trimmed) < 7 || !strings.EqualFold(trimmed[:6], "Bearer") || trimmed[6] != ' ' {
		return "", false
	}
	return strings.TrimSpace(trimmed[7:]), true
}

func looksLikeBearer(s string) bool {
	trimmed := strings.TrimSpace(s)
	if len(trimmed) < 6 {
		return false
	}
	return strings.EqualFold(trimmed[:6], "Bearer")
}

func listCredentialsHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		resp, err := ListCredentials(ctx, d, CredentialListReq{
			SAID:     string(c.Query("sa_id")),
			Protocol: string(c.Query("protocol")),
			Resource: string(c.Query("resource")),
		})
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, resp)
	}
}

func credentialGetHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		resp, err := GetCredential(ctx, d, c.Param("id"))
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, resp)
	}
}

func credentialRotateHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		resp, err := RotateCredential(ctx, d, c.Param("id"))
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, resp)
	}
}

func credentialRevokeHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		resp, err := RevokeCredential(ctx, d, c.Param("id"))
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, resp)
	}
}
