package admin

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func bucketSetPolicyHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		var req BucketPolicySetReq
		body := c.Request.Body()
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(c, NewInvalid("invalid JSON body: "+err.Error()))
				return
			}
		}
		if req.Policy == nil || bytes.Equal(req.Policy, []byte("null")) {
			writeError(c, NewInvalid("policy field is required"))
			return
		}
		if err := AdminSetBucketPolicy(ctx, d, name, req); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func bucketDeletePolicyHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		if err := AdminDeleteBucketPolicy(ctx, d, name); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func bucketSetVersioningHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		var req BucketVersioningSetReq
		body := c.Request.Body()
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(c, NewInvalid("invalid JSON body: "+err.Error()))
				return
			}
		}
		if err := AdminSetBucketVersioning(ctx, d, name, req); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func bucketDeleteHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		force := string(c.Query("force")) == "true"
		if err := AdminDeleteBucket(ctx, d, name, force); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}
