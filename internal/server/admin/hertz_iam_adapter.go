package admin

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/gritive/GrainFS/internal/iam"
)

func iamGetSAHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		resp, err := GetSA(ctx, d, c.Param("id"))
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, resp)
	}
}

func iamGetBucketUpstreamHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		resp, err := GetBucketUpstream(ctx, d, c.Param("bucket"))
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, resp)
	}
}

func iamBucketUpstreamCutoverHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		var req iam.BucketUpstreamCutoverRequest
		if err := decodeOptionalJSON(c.Request.Body(), &req); err != nil {
			writeError(c, err)
			return
		}
		if err := CutoverBucketUpstream(ctx, d, req.Bucket); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func iamDeleteSAHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if err := DeleteSA(ctx, d, c.Param("id")); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func iamCreateKeyHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		saID := c.Param("id")
		var req iam.KeyCreateRequest
		if err := decodeOptionalJSON(c.Request.Body(), &req); err != nil {
			writeError(c, err)
			return
		}
		resp, err := CreateKey(ctx, d, saID, req)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusCreated, resp)
	}
}

func iamRevokeKeyHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if err := RevokeKey(ctx, d, c.Param("id"), c.Param("ak")); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func iamDeleteGrantHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		var req iam.GrantDeleteRequest
		if err := decodeOptionalJSON(c.Request.Body(), &req); err != nil {
			writeError(c, err)
			return
		}
		if err := DeleteGrant(ctx, d, req); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func iamListGrantsHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		sa := string(c.Query("sa"))
		bucket := string(c.Query("bucket"))
		resp, err := ListGrants(ctx, d, sa, bucket)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, resp)
	}
}

func iamDeleteBucketUpstreamHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if err := DeleteBucketUpstream(ctx, d, c.Param("bucket")); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}
