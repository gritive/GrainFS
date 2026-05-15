package server

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func (s *Server) registerAlertsAPI(h *server.Hertz) {
	if !s.routeFeatureRoutesVisible(routeFeatureAlerts) {
		return
	}
	h.GET(routePathAlertsStatus, localhostOnly(), s.alertsStatus)
	h.POST(routePathAlertsResend, localhostOnly(), s.alertsResend)
}

func (s *Server) alertsStatus(_ context.Context, c *app.RequestContext) {
	c.JSON(consts.StatusOK, s.alertsStatusSnapshot())
}

func (s *Server) alertsResend(_ context.Context, c *app.RequestContext) {
	if s.blockIfMutationDisabled(c, "alerts_resend") {
		return
	}
	resent, err := s.resendLastFailedAlert()
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
