package server

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/rs/zerolog/log"
)

func (s *Server) serveDashboard(ctx context.Context, c *app.RequestContext) {
	data, err := uiHTML.ReadFile("ui/index.html")
	if err != nil {
		c.String(consts.StatusInternalServerError, "UI not found")
		return
	}
	c.SetContentType("text/html; charset=utf-8")
	c.SetStatusCode(consts.StatusOK)
	if _, err := c.Write(data); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("write dashboard response")
	}
}
