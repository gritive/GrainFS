package admin

import (
	"context"
	"encoding/json"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func iamPolicyPutHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		body := c.Request.Body()
		if err := PutPolicy(ctx, d, name, body); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func iamPolicyGetHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		raw, err := GetPolicy(ctx, d, name)
		if err != nil {
			writeError(c, err)
			return
		}
		c.Data(consts.StatusOK, "application/json", raw)
	}
}

func iamPolicyListHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		names, err := ListPolicies(ctx, d)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, names)
	}
}

func iamPolicyDeleteHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		if err := DeletePolicy(ctx, d, name); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func iamPolicyAttachSAHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		said := c.Param("said")
		if err := AttachPolicyToSA(ctx, d, name, said); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func iamPolicyDetachSAHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		said := c.Param("said")
		if err := DetachPolicyFromSA(ctx, d, name, said); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func iamPolicySimulateHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		var req PolicySimulateRequest
		if body := c.Request.Body(); len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(c, NewInvalid("invalid JSON body: "+err.Error()))
				return
			}
		}
		result, err := SimulatePolicy(ctx, d, req)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, result)
	}
}
