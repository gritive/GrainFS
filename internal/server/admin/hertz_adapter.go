package admin

import (
	"context"
	"encoding/json"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// --- Generic wrapper helpers ---

func wrapZero[Resp any](d *Deps, fn func(context.Context, *Deps) (Resp, error)) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		resp, err := fn(ctx, d)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, resp)
	}
}

func wrapName[Resp any](d *Deps, fn func(context.Context, *Deps, string) (Resp, error)) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		resp, err := fn(ctx, d, name)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, resp)
	}
}

func wrapBody[Req any, Resp any](d *Deps, fn func(context.Context, *Deps, Req) (Resp, error)) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		var req Req
		body := c.Request.Body()
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(c, NewInvalid("invalid JSON body: "+err.Error()))
				return
			}
		}
		resp, err := fn(ctx, d, req)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusCreated, resp)
	}
}

func wrapBodyNoOut[Req any](d *Deps, fn func(context.Context, *Deps, Req) error) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		var req Req
		body := c.Request.Body()
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(c, NewInvalid("invalid JSON body: "+err.Error()))
				return
			}
		}
		if err := fn(ctx, d, req); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusCreated)
	}
}

// wrapBodyNoOut204 is like wrapBodyNoOut but returns 204 No Content on success.
// Use for idempotent upsert routes (PUT) where creating vs. updating is not
// distinguishable at the transport layer.
func wrapBodyNoOut204[Req any](d *Deps, fn func(context.Context, *Deps, Req) error) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		var req Req
		body := c.Request.Body()
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(c, NewInvalid("invalid JSON body: "+err.Error()))
				return
			}
		}
		if err := fn(ctx, d, req); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func wrapNameBody[Req any, Resp any](d *Deps, fn func(context.Context, *Deps, string, Req) (Resp, error)) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		var req Req
		body := c.Request.Body()
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(c, NewInvalid("invalid JSON body: "+err.Error()))
				return
			}
		}
		resp, err := fn(ctx, d, name, req)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, resp)
	}
}
