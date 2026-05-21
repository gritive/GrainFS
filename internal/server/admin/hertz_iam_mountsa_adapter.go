package admin

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func iamMountSAPostHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		var req CreateMountSAReq
		if err := decodeOptionalJSON(c.Request.Body(), &req); err != nil {
			writeError(c, err)
			return
		}
		item, err := CreateMountSA(ctx, d, req)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusCreated, item)
	}
}

func iamMountSAListHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		items, err := ListMountSAs(ctx, d)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, items)
	}
}

func iamMountSAGetHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		item, err := GetMountSA(ctx, d, c.Param("name"))
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, item)
	}
}

func iamMountSADeleteHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if err := DeleteMountSA(ctx, d, c.Param("name")); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func iamMountSAPolicyAttachHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if err := AttachPolicyToMountSA(ctx, d, c.Param("policy"), c.Param("name")); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func iamMountSAPolicyDetachHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if err := DetachPolicyFromMountSA(ctx, d, c.Param("policy"), c.Param("name")); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}
