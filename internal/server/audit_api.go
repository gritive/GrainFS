package server

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

type auditHealthResponse struct {
	Enabled          bool   `json:"enabled"`
	GuaranteeState   string `json:"guarantee_state"`
	OutboxBacklog    int    `json:"outbox_backlog"`
	OldestPendingUS  int64  `json:"oldest_pending_us,omitempty"`
	LastCommitUnixUS int64  `json:"last_commit_unix_us,omitempty"`
}

func (s *Server) registerAuditAPI(h *server.Hertz) {
	h.GET("/api/audit/health", localhostOnly(), s.auditHealth)
}

func (s *Server) auditHealth(ctx context.Context, c *app.RequestContext) {
	if s.auditOutbox == nil {
		c.JSON(consts.StatusOK, auditHealthResponse{
			Enabled:        false,
			GuaranteeState: "disabled",
		})
		return
	}

	stats, err := s.auditOutbox.Stats(ctx)
	if err != nil {
		c.JSON(consts.StatusOK, auditHealthResponse{
			Enabled:        true,
			GuaranteeState: "critical",
		})
		return
	}

	c.JSON(consts.StatusOK, auditHealthResponse{
		Enabled:         true,
		GuaranteeState:  "ok",
		OutboxBacklog:   stats.Backlog,
		OldestPendingUS: stats.OldestPendingUS,
	})
}
