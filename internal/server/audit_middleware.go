package server

import (
	"context"
	"net/http"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/google/uuid"

	"github.com/gritive/GrainFS/internal/audit"
)

// auditRequestID returns the rid attached by WithRequestID, falling back to
// a fresh UUID if the middleware was not wired (defensive — production
// installMiddlewares always installs WithRequestID first, but a handful of
// in-process test fixtures construct *Server without it).
func auditRequestID(ctx context.Context) string {
	if rid := RequestIDFromContext(ctx); rid != "" {
		return rid
	}
	return uuid.NewString()
}

const auditErrReasonKey = "audit.err_reason"
const auditObjectKeyKey = "audit.object_key"
const auditBytesOutKey = "audit.bytes_out"

func (s *Server) auditEnvelopeMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if !s.auditSinkConfigured() {
			c.Next(ctx)
			return
		}
		bucket := c.Param("bucket")
		if bucket == "" {
			c.Next(ctx)
			return
		}
		if bucket == audit.BucketName && AccessKeyFromContext(ctx) == s.auditInternalAccessKey {
			c.Next(ctx)
			return
		}

		key := getKey(c)
		requestID := auditRequestID(ctx)
		// WithRequestID already dual-wrote X-GrainFS-Request-Id and
		// x-amz-request-id; re-set defensively for the fallback path.
		c.Header("x-amz-request-id", requestID)
		start := time.Now()
		ev := s.newAuditEnvelopeEvent(ctx, c, auditEnvelopeInput{
			bucket:    bucket,
			key:       key,
			requestID: requestID,
			start:     start,
		})

		if err := s.appendAuditAttempt(ctx, ev); err != nil {
			writeXMLError(c, consts.StatusServiceUnavailable, "AuditUnavailable", "audit log unavailable")
			c.Abort()
			return
		}

		c.Next(ctx)

		ev = finalizeAuditEnvelopeEvent(c, ev, start)
		if ev.Status >= 400 {
			if ev.Status == http.StatusUnauthorized || ev.Status == http.StatusForbidden {
				ev.AuthStatus = "deny"
			} else {
				ev.AuthStatus = "error"
			}
			ev.ErrClass = http.StatusText(int(ev.Status))
			if ev.ErrClass == "" {
				ev.ErrClass = "Error"
			}
			if reason, ok := c.Get(auditErrReasonKey); ok {
				if s, ok := reason.(string); ok {
					ev.ErrReason = s
				}
			}
			if ev.ErrReason == "" && ev.AuthStatus == "deny" {
				ev.AuthStatus = "error"
			}
		}
		ev = normalizeAuditEvent(ev)
		s.finalizeAuditEvent(context.Background(), ev)
	}
}

func (s *Server) recordAuditAuthFailure(ctx context.Context, c *app.RequestContext, status int, reason string) {
	if !s.auditSinkConfigured() {
		return
	}
	path := string(c.URI().Path())
	bucket := c.Param("bucket")
	key := getKey(c)
	if bucket == "" {
		bucket, key = s3PathBucketKey(path)
	}
	if bucket == "" || bucket == audit.BucketName {
		return
	}
	requestID := auditRequestID(ctx)
	c.Header("x-amz-request-id", requestID)
	ev := s.newAuditAuthFailureEvent(ctx, c, bucket, key, requestID, status, reason)
	s.appendFinalizedAuditEvent(context.Background(), ev)
}
