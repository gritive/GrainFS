package admin

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func deleteVolumeHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		resp, err := DeleteVolume(ctx, d, name)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, resp)
	}
}
