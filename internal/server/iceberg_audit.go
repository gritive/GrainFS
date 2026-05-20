package server

import (
	"context"
	"net/http"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/google/uuid"

	"github.com/gritive/GrainFS/internal/audit"
	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
	"github.com/gritive/GrainFS/internal/iam/policy"
)

// emitIcebergAuditAllow emits an audit.s3 allow row for a bearer-authenticated
// Iceberg request. Called after the downstream handler returns so the HTTP
// status code is captured from the response. evalResult may be nil when no
// policy authorizer is configured (JWT verify is the only gate).
func (s *Server) emitIcebergAuditAllow(ctx context.Context, c *app.RequestContext, action string, claims *iamjwt.Claims, evalResult *policy.EvalResult, start time.Time) {
	if !s.auditSinkConfigured() {
		return
	}
	status := int32(c.Response.StatusCode())
	if status == 0 {
		status = int32(http.StatusOK)
	}
	latencyUS := int32(time.Since(start).Microseconds())
	ev := audit.S3Event{
		Ts:             start.UnixMicro(),
		EventID:        uuid.NewString(),
		NodeID:         s.auditNodeID,
		RequestID:      RequestIDFromContext(ctx),
		SAID:           icebergSAID(claims),
		SourceIP:       s.authoritativeClientIP(c),
		Method:         auditString8(string(c.Method())),
		Operation:      action,
		Bucket:         icebergWarehouse(claims),
		Status:         status,
		AuthStatus:     "allow",
		LatencyMs:      int32(time.Since(start).Milliseconds()),
		AuthzLatencyUS: latencyUS,
	}
	if evalResult != nil {
		ev.MatchedPolicyID = evalResult.MatchedPolicy
		ev.MatchedSID = evalResult.MatchedSid
		ev.ConditionContext = evalResult.ConditionContext
	}
	s.appendFinalizedAuditEvent(context.Background(), normalizeAuditEvent(ev))
}

// emitIcebergAuditAnonAllow emits an audit.s3 anon_allow row for an Iceberg
// request that was short-circuited by iam.anon-enabled=true. Called after the
// downstream handler returns.
func (s *Server) emitIcebergAuditAnonAllow(ctx context.Context, c *app.RequestContext, action string, start time.Time) {
	if !s.auditSinkConfigured() {
		return
	}
	status := int32(c.Response.StatusCode())
	if status == 0 {
		status = int32(http.StatusOK)
	}
	ev := normalizeAuditEvent(audit.S3Event{
		Ts:         start.UnixMicro(),
		EventID:    uuid.NewString(),
		NodeID:     s.auditNodeID,
		RequestID:  RequestIDFromContext(ctx),
		SAID:       audit.AnonSAID,
		SourceIP:   s.authoritativeClientIP(c),
		Method:     auditString8(string(c.Method())),
		Operation:  action,
		Status:     status,
		AuthStatus: "anon_allow",
		LatencyMs:  int32(time.Since(start).Milliseconds()),
	})
	s.appendFinalizedAuditEvent(context.Background(), ev)
}

// emitIcebergAuditDeny emits an audit.s3 deny row for a bearer-gated Iceberg
// request that was rejected. Called synchronously before the response is
// finalized (the status code passed in is the one written to the response).
// evalResult may be nil for pre-policy denials (token invalid, warehouse mismatch).
// start is threaded from icebergGuarded so Ts and LatencyMs share the same baseline.
func (s *Server) emitIcebergAuditDeny(ctx context.Context, c *app.RequestContext, action string, saID string, warehouse string, status int, reason string, evalResult *policy.EvalResult, start time.Time) {
	if !s.auditSinkConfigured() {
		return
	}
	latencyUS := int32(time.Since(start).Microseconds())
	ev := audit.S3Event{
		Ts:             start.UnixMicro(),
		EventID:        uuid.NewString(),
		NodeID:         s.auditNodeID,
		RequestID:      RequestIDFromContext(ctx),
		SAID:           saID,
		SourceIP:       s.authoritativeClientIP(c),
		Method:         auditString8(string(c.Method())),
		Operation:      action,
		Bucket:         warehouse,
		Status:         int32(status),
		AuthStatus:     "deny",
		ErrClass:       http.StatusText(status),
		ErrReason:      reason,
		LatencyMs:      int32(time.Since(start).Milliseconds()),
		AuthzLatencyUS: latencyUS,
	}
	if evalResult != nil {
		ev.MatchedPolicyID = evalResult.MatchedPolicy
		ev.MatchedSID = evalResult.MatchedSid
		ev.ConditionContext = evalResult.ConditionContext
	}
	s.appendFinalizedAuditEvent(context.Background(), normalizeAuditEvent(ev))
}

// icebergSAID returns the JWT sub claim or "" when claims is nil.
func icebergSAID(claims *iamjwt.Claims) string {
	if claims == nil {
		return ""
	}
	return claims.Sub
}

// icebergWarehouse returns the JWT warehouse claim or "" when claims is nil.
func icebergWarehouse(claims *iamjwt.Claims) string {
	if claims == nil {
		return ""
	}
	return claims.Warehouse
}
