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
	actor := adminActorMiddleware(d)
	configListAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:AdminConfigList",
		resource: adminConfigResource,
	})
	configReadAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:AdminConfigRead",
		resource: adminConfigResource,
	})
	configWriteAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:AdminConfigWrite",
		resource: adminConfigResource,
	})
	configDeleteAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:AdminConfigDelete",
		resource: adminConfigResource,
	})
	g.GET(routePathConfig, actor, configListAuthz, configListHandler(d))
	g.GET(routePathConfigByKey, actor, configReadAuthz, configGetHandler(d))
	g.PUT(routePathConfigByKey, actor, configWriteAuthz, configSetHandler(d))
	g.DELETE(routePathConfigByKey, actor, configDeleteAuthz, configUnsetHandler(d))
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
