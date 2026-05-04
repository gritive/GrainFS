package server

import (
	"context"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/gritive/GrainFS/internal/dashboard"
)

// DashboardTokenMiddleware authenticates requests to /ui/* and /ui/api/*
// against the dashboard auth token (Authorization: Bearer <token>).
//
// As a special case, the bare /ui/ entry path also accepts ?token=... so the
// initial URL handed out by `grainfs dashboard` (with token in fragment) can
// alternatively pass it via query if the browser strips the fragment. The
// browser is expected to copy the token into localStorage on first load and
// send it in the Authorization header on subsequent fetches.
//
// Paths outside /ui/ are passed through unchanged.
func DashboardTokenMiddleware(ts *dashboard.TokenStore) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if ts == nil {
			c.Next(ctx)
			return
		}
		path := string(c.Path())
		if !strings.HasPrefix(path, "/ui/") && path != "/ui" {
			c.Next(ctx)
			return
		}
		// Allow ?token=... on the entry path so the browser can hand off.
		if path == "/ui/" || path == "/ui" {
			if t := string(c.Query("token")); t != "" && ts.Verify(t) {
				c.Next(ctx)
				return
			}
		}
		auth := string(c.Request.Header.Get("Authorization"))
		if strings.HasPrefix(auth, "Bearer ") && ts.Verify(auth[len("Bearer "):]) {
			c.Next(ctx)
			return
		}
		c.JSON(consts.StatusUnauthorized, map[string]string{
			"error": "unauthorized; run `grainfs dashboard` for a valid URL",
			"code":  "unauthorized",
		})
		c.Abort()
	}
}
