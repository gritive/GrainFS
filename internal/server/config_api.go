package server

import (
	"context"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

type configPatchRequest struct {
	ScrubInterval     string `json:"scrub_interval"`
	LifecycleInterval string `json:"lifecycle_interval"`
}

type configPatchResponse struct {
	ScrubInterval     string `json:"scrub_interval,omitempty"`
	LifecycleInterval string `json:"lifecycle_interval,omitempty"`
	Applied           bool   `json:"applied"`
}

// registerConfigAPI registers PATCH /api/admin/config for hot-reload.
func (s *Server) registerConfigAPI(h *server.Hertz) {
	h.PATCH("/api/admin/config", localhostOnly(), s.patchConfigHandler)
}

func (s *Server) patchConfigHandler(_ context.Context, c *app.RequestContext) {
	var req configPatchRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}

	resp := configPatchResponse{Applied: true}

	if req.ScrubInterval != "" {
		d, err := time.ParseDuration(req.ScrubInterval)
		if err != nil || d <= 0 {
			c.JSON(consts.StatusBadRequest, map[string]string{"error": "invalid scrub_interval"})
			return
		}
		if s.scrubber != nil {
			s.scrubber.SetInterval(d)
		}
		resp.ScrubInterval = d.String()
	}

	c.JSON(consts.StatusOK, resp)
}
