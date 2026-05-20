package admin

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/auditadmin"
)

// AuditQueryService is the slim interface audit admin handlers need.
// Satisfied by *audit.DuckDBSearcher; nil disables audit admin endpoints.
type AuditQueryService interface {
	Query(ctx context.Context, sql string, limit int) (columns []string, rows [][]string, err error)
	SearchS3(ctx context.Context, f audit.SearchFilter) ([]audit.SearchRow, error)
}

func auditQueryHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if d.AuditQuery == nil {
			writeError(c, NewUnavailable("audit search not configured"))
			return
		}
		var req auditadmin.QueryRequest
		if err := decodeOptionalJSON(c.Request.Body(), &req); err != nil {
			writeError(c, err)
			return
		}
		sql := strings.TrimSpace(req.SQL)
		if sql == "" {
			writeError(c, NewInvalid("sql is required"))
			return
		}
		cols, rows, err := d.AuditQuery.Query(ctx, sql, audit.ClampSearchLimit(req.Limit))
		if err != nil {
			writeError(c, NewInvalid(fmt.Sprintf("query error: %v", err)))
			return
		}
		writeOK(c, consts.StatusOK, auditadmin.QueryResponse{Columns: cols, Rows: rows})
	}
}

func auditRecentDeniesHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if d.AuditQuery == nil {
			writeError(c, NewUnavailable("audit search not configured"))
			return
		}
		limit := audit.MaxSearchLimit
		if v := string(c.Query("limit")); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				limit = audit.ClampSearchLimit(n)
			}
		}
		sql := fmt.Sprintf(
			`SELECT ts, sa_id, operation, bucket, key, matched_sid `+
				`FROM grainfs_iceberg.audit.s3 `+
				`WHERE auth_status = 'deny' `+
				`ORDER BY ts DESC LIMIT %d`,
			limit,
		)
		cols, rows, err := d.AuditQuery.Query(ctx, sql, 0)
		if err != nil {
			writeError(c, fmt.Errorf("query error: %w", err))
			return
		}
		writeOK(c, consts.StatusOK, auditadmin.QueryResponse{Columns: cols, Rows: rows})
	}
}

func auditBySAHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if d.AuditQuery == nil {
			writeError(c, NewUnavailable("audit search not configured"))
			return
		}
		said := c.Param("said")
		if said == "" {
			writeError(c, NewInvalid("sa_id is required"))
			return
		}
		limit := audit.MaxSearchLimit
		if v := string(c.Query("limit")); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				limit = audit.ClampSearchLimit(n)
			}
		}
		f := audit.SearchFilter{SAID: said, Limit: limit}
		dbRows, err := d.AuditQuery.SearchS3(ctx, f)
		if err != nil {
			writeError(c, fmt.Errorf("query error: %w", err))
			return
		}
		writeOK(c, consts.StatusOK, searchRowsToQueryResponse(dbRows))
	}
}

func auditByRequestIDHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if d.AuditQuery == nil {
			writeError(c, NewUnavailable("audit search not configured"))
			return
		}
		rid := c.Param("rid")
		if rid == "" {
			writeError(c, NewInvalid("request_id is required"))
			return
		}
		f := audit.SearchFilter{RequestID: rid, Limit: audit.MaxSearchLimit}
		dbRows, err := d.AuditQuery.SearchS3(ctx, f)
		if err != nil {
			writeError(c, fmt.Errorf("query error: %w", err))
			return
		}
		writeOK(c, consts.StatusOK, searchRowsToQueryResponse(dbRows))
	}
}

func searchRowsToQueryResponse(dbRows []audit.SearchRow) auditadmin.QueryResponse {
	cols := []string{
		"ts", "request_id", "sa_id", "source_ip", "user_agent",
		"operation", "bucket", "key", "http_status", "err_class", "err_reason", "latency_ms",
	}
	rows := make([][]string, 0, len(dbRows))
	for _, r := range dbRows {
		rows = append(rows, []string{
			r.Ts.Format("2006-01-02T15:04:05Z"),
			r.RequestID,
			r.SAID,
			r.SourceIP,
			r.UserAgent,
			r.Operation,
			r.Bucket,
			r.Key,
			strconv.Itoa(r.Status),
			r.ErrClass,
			r.ErrReason,
			strconv.Itoa(r.LatencyMs),
		})
	}
	return auditadmin.QueryResponse{Columns: cols, Rows: rows}
}
