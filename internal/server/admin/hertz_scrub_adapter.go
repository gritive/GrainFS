package admin

import (
	"context"
	"encoding/json"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func scrubVolumeHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		var req ScrubVolumeReq
		body := c.Request.Body()
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(c, NewInvalid("invalid JSON body: "+err.Error()))
				return
			}
		}
		req.Name = c.Param("name")
		resp, err := ScrubVolume(ctx, d, req)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusCreated, resp)
	}
}

func scrubJobByIDHandler(d *Deps, fn func(context.Context, *Deps, string) (ScrubJobInfo, error)) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		id := c.Param("id")
		resp, err := fn(ctx, d, id)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, resp)
	}
}

func scrubJobCancelHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		id := c.Param("id")
		if err := CancelScrubJob(ctx, d, id); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}
