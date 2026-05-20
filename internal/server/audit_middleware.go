package server

import (
	"context"
	"net/http"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/audit"
)

const auditErrReasonKey = "audit.err_reason"
const auditObjectKeyKey = "audit.object_key"
const auditBytesOutKey = "audit.bytes_out"

// auditAuthzDecisionKey carries the s3auth.Decision (specifically its Detail
// and AuthzLatencyUS) emitted by RequestAuthorizer.Decide so the audit
// envelope finalizer can populate matched_policy_id / matched_sid /
// authz_latency_us / condition_context_json on the audit.s3 row. T51' §6.
const auditAuthzDecisionKey = "audit.authz_decision"

// auditAuthzAnonAllowKey is a small specialization that lets the envelope
// finalizer distinguish anon-allow from authenticated allow without
// reparsing the policy reason. T51' §6.
const auditAuthzAnonAllowKey = "audit.authz_anon_allow"

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
		// WithRequestID runs first in installMiddlewares and is the single
		// source of truth for rid; the response header was set there too.
		requestID := RequestIDFromContext(ctx)
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
	requestID := RequestIDFromContext(ctx)
	ev := s.newAuditAuthFailureEvent(ctx, c, bucket, key, requestID, status, reason)
	s.appendFinalizedAuditEvent(context.Background(), ev)
}
