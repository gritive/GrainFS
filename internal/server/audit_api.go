package server

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/audit"
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
	h.GET("/api/audit/s3", localhostOnly(), s.auditSearchS3)
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

func (s *Server) auditSearchS3(ctx context.Context, c *app.RequestContext) {
	if s.auditSearcher == nil {
		c.JSON(consts.StatusServiceUnavailable, map[string]string{"error": "audit search is not configured"})
		return
	}
	filter, err := parseAuditSearchFilter(c)
	if err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	rows, err := s.auditSearcher.SearchS3(queryCtx, filter)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	c.JSON(consts.StatusOK, rows)
}

func parseAuditSearchFilter(c *app.RequestContext) (audit.SearchFilter, error) {
	filter := audit.SearchFilter{
		Since: time.Now().Add(-time.Hour),
		Limit: audit.MaxSearchLimit,
	}
	if since := queryString(c, "since"); since != "" {
		ts, err := time.Parse(time.RFC3339Nano, since)
		if err != nil {
			return audit.SearchFilter{}, fmt.Errorf("invalid since")
		}
		filter.Since = ts
	}
	if until := queryString(c, "until"); until != "" {
		ts, err := time.Parse(time.RFC3339Nano, until)
		if err != nil {
			return audit.SearchFilter{}, fmt.Errorf("invalid until")
		}
		filter.Until = ts
	}
	filter.Bucket = queryString(c, "bucket")
	filter.KeyPrefix = queryString(c, "key_prefix")
	filter.SAID = queryString(c, "sa_id")
	filter.Operation = queryString(c, "operation")
	filter.ErrClass = queryString(c, "err_class")
	filter.RequestID = queryString(c, "request_id")

	if raw := queryString(c, "status"); raw != "" {
		status, err := strconv.Atoi(raw)
		if err != nil || status < 100 || status > 599 {
			return audit.SearchFilter{}, fmt.Errorf("invalid status")
		}
		filter.Status = status
	}
	if raw := queryString(c, "status_class"); raw != "" {
		statusClass, err := strconv.Atoi(raw)
		if err != nil || statusClass < 100 || statusClass > 500 || statusClass%100 != 0 {
			return audit.SearchFilter{}, fmt.Errorf("invalid status_class")
		}
		filter.StatusClass = statusClass
	}
	if raw := queryString(c, "limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil {
			return audit.SearchFilter{}, fmt.Errorf("invalid limit")
		}
		filter.Limit = audit.ClampSearchLimit(limit)
	}
	return filter, nil
}

func queryString(c *app.RequestContext, name string) string {
	return string(c.QueryArgs().Peek(name))
}
