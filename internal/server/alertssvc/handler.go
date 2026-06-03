package alertssvc

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	hertz "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// Deps carries everything the alerts handlers need from core: the State (a
// concrete pointer, cheap to share) plus closures so alertssvc never imports
// server.
type Deps struct {
	State            *State
	LocalhostOnly    func() app.HandlerFunc
	MutationDisabled func(c *app.RequestContext, operation string) bool
	FeatureVisible   func() bool
	StatusPath       string
	ResendPath       string
}

type Handler struct {
	deps Deps
}

func NewHandler(d Deps) *Handler {
	return &Handler{deps: d}
}

// Register wires the alerts endpoints onto hz when alerts are configured and
// the feature is visible. No-op otherwise (matches the original
// routeFeatureRoutesVisible gate).
func (h *Handler) Register(hz *hertz.Hertz) {
	if h.deps.State == nil || !h.deps.FeatureVisible() {
		return
	}
	hz.GET(h.deps.StatusPath, h.deps.LocalhostOnly(), h.status)
	hz.POST(h.deps.ResendPath, h.deps.LocalhostOnly(), h.resend)
}

func (h *Handler) status(_ context.Context, c *app.RequestContext) {
	c.JSON(consts.StatusOK, h.deps.State.StatusSnapshot())
}

func (h *Handler) resend(_ context.Context, c *app.RequestContext) {
	if h.deps.MutationDisabled(c, "alerts_resend") {
		return
	}
	resent, err := h.deps.State.ResendLastFailed()
	if err != nil {
		c.JSON(consts.StatusBadGateway, map[string]any{"resent": false, "error": err.Error()})
		return
	}
	if !resent {
		c.JSON(consts.StatusOK, map[string]any{"resent": false, "reason": "no failed alert to resend"})
		return
	}
	c.JSON(consts.StatusOK, map[string]any{"resent": true})
}
