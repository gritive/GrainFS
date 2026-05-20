package server

import (
	"context"
	"encoding/json"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// registerLifecycleTestCtlAPI wires the e2e-only deterministic-drive endpoints.
// These exist because the e2e harness spawns grainfs as an external binary —
// the test process cannot reach the in-process *lifecycle.Service directly, so
// it drives it across the HTTP boundary instead.
//
// The endpoints are SigV4-authenticated like the rest of /api/. They are
// always registered; they no-op (503) when lifecycle is disabled. They are
// not protected behind a build tag — the lifecycle worker package already
// gates SetNowForTest / RunCycleForTest via "ForTest" naming and the route
// surface is innocuous (a follower's RunCycleForTest is a no-op, and SetNow
// only affects the in-process clock source which doesn't escape the process).
func (s *Server) registerLifecycleTestCtlAPI(h *server.Hertz) {
	h.POST(routePathLifecycleTestRunCycle, s.lifecycleTestRunCycleHandler)
	h.POST(routePathLifecycleTestSetNow, s.lifecycleTestSetNowHandler)
}

func (s *Server) lifecycleTestRunCycleHandler(ctx context.Context, c *app.RequestContext) {
	if !s.routeFeatureAvailable(routeFeatureLifecycle) {
		c.JSON(consts.StatusServiceUnavailable, map[string]any{"enabled": false})
		return
	}
	s.lifecycle.RunCycleForTest(ctx)
	c.JSON(consts.StatusOK, map[string]any{"ok": true})
}

type lifecycleTestSetNowRequest struct {
	UnixNano int64 `json:"unix_nano"`
}

func (s *Server) lifecycleTestSetNowHandler(_ context.Context, c *app.RequestContext) {
	if !s.routeFeatureAvailable(routeFeatureLifecycle) {
		c.JSON(consts.StatusServiceUnavailable, map[string]any{"enabled": false})
		return
	}
	var req lifecycleTestSetNowRequest
	if err := json.Unmarshal(c.Request.Body(), &req); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	t := time.Unix(0, req.UnixNano)
	s.lifecycle.SetNowForTest(func() time.Time { return t })
	c.JSON(consts.StatusOK, map[string]any{"ok": true, "unix_nano": req.UnixNano})
}
