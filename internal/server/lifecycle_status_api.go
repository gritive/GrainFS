package server

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// registerLifecycleStatusAPI wires GET /api/cluster/lifecycle/status. The route
// is always registered; when the lifecycle service is disabled
// (s.lifecycle == nil) the handler returns 503 {"enabled": false}.
func (s *Server) registerLifecycleStatusAPI(h *server.Hertz) {
	h.GET(routePathLifecycleStatus, s.lifecycleStatusHandler)
}

func (s *Server) lifecycleStatusHandler(_ context.Context, c *app.RequestContext) {
	if !s.routeFeatureAvailable(routeFeatureLifecycle) {
		c.JSON(consts.StatusServiceUnavailable, map[string]any{"enabled": false})
		return
	}
	c.JSON(consts.StatusOK, s.lifecycleStatusSnapshot())
}
