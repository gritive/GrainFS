package incidentsvc

import (
	"context"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/incident"
)

// Deps carries what the incident read-API needs from core, so this package
// never imports `server` (one-way edge).
type Deps struct {
	IncidentStore    incident.StateStore
	FeatureAvailable func() bool
}

type Handler struct {
	deps Deps
}

func NewHandler(d Deps) *Handler { return &Handler{deps: d} }

// Register wires the incident read endpoints. When the feature is unavailable
// the routes are not registered (404) — same fail-closed behavior as before.
func (h *Handler) Register(hz *server.Hertz, path, prefix string) {
	if !h.deps.FeatureAvailable() {
		return
	}
	hz.GET(path, h.listIncidents)
	hz.GET(prefix+":id", h.getIncident)
}

func (h *Handler) listIncidents(ctx context.Context, c *app.RequestContext) {
	limit := 50
	if raw := string(c.Query("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 500 {
			c.JSON(consts.StatusBadRequest, map[string]string{"error": "limit must be between 1 and 500"})
			return
		}
		limit = parsed
	}
	states, err := h.deps.IncidentStore.List(ctx, limit)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	c.JSON(consts.StatusOK, states)
}

func (h *Handler) getIncident(ctx context.Context, c *app.RequestContext) {
	state, ok, err := h.deps.IncidentStore.Get(ctx, c.Param("id"))
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
