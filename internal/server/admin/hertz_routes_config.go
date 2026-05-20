package admin

import (
	"context"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/gritive/GrainFS/internal/adminapi"
)

func registerConfig(g router, d *Deps) {
	if d == nil || d.ConfigProposer == nil || d.ConfigStore == nil {
		return
	}
	g.GET(routePathConfig, configListHandler(d))
	g.GET(routePathConfigByKey, configGetHandler(d))
	g.PUT(routePathConfigByKey, configSetHandler(d))
	g.DELETE(routePathConfigByKey, configUnsetHandler(d))
}

func configListHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		allStr := string(c.Query("all"))
		all, _ := strconv.ParseBool(allStr)
		entries, err := ConfigListEntries(ctx, d, all)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, entries)
	}
}

func configGetHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		key := c.Param("key")
		entry, err := ConfigGetEntry(ctx, d, key)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, entry)
	}
}

func configSetHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		key := c.Param("key")
		var req adminapi.ConfigSetReq
		if err := decodeOptionalJSON(c.Request.Body(), &req); err != nil {
			writeError(c, err)
			return
		}
		if err := ConfigSetEntry(ctx, d, key, req.Value); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func configUnsetHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		key := c.Param("key")
		if err := ConfigUnsetEntry(ctx, d, key); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}
