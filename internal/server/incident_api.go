package server

import (
	"context"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func (s *Server) registerIncidentAPI(h *server.Hertz) {
	if s.incidentStore == nil {
		return
	}
	h.GET("/api/incidents", s.listIncidents)
	h.GET("/api/incidents/:id", s.getIncident)
}

func (s *Server) listIncidents(ctx context.Context, c *app.RequestContext) {
	limit := 50
	if raw := string(c.Query("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 500 {
			c.JSON(consts.StatusBadRequest, map[string]string{"error": "limit must be between 1 and 500"})
			return
		}
		limit = parsed
	}
	states, err := s.incidentStore.List(ctx, limit)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	c.JSON(consts.StatusOK, states)
}

func (s *Server) getIncident(ctx context.Context, c *app.RequestContext) {
	state, ok, err := s.incidentStore.Get(ctx, c.Param("id"))
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if !ok {
		c.JSON(consts.StatusNotFound, map[string]string{"error": "incident not found"})
		return
	}
	c.JSON(consts.StatusOK, state)
}
