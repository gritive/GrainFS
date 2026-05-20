package server

import (
	"context"
	"net/http"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/google/uuid"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/s3auth"
)

type auditEnvelopeInput struct {
	bucket    string
	key       string
	requestID string
	start     time.Time
}

func (s *Server) newAuditEnvelopeEvent(ctx context.Context, c *app.RequestContext, in auditEnvelopeInput) audit.S3Event {
	op := audit.ClassifyS3Operation(string(c.Method()), in.key != "", string(c.URI().QueryString()), toHTTPRequest(c).Header)
	ev := audit.S3Event{
		Ts:          in.start.UnixMicro(),
		EventID:     uuid.NewString(),
		NodeID:      s.auditNodeID,
		RequestID:   in.requestID,
		SAID:        iam.PrincipalFromContext(ctx),
		SourceIP:    s.authoritativeClientIP(c),
		UserAgent:   auditString16(string(c.UserAgent())),
		Method:      auditString8(string(c.Method())),
		Operation:   op.Operation,
		Bucket:      in.bucket,
		Key:         in.key,
		Subresource: op.Subresource,
		AuthStatus:  "allow",
		BytesIn:     int64(len(c.Request.Body())),
		VersionID:   string(c.QueryArgs().Peek("versionId")),
		UploadID:    string(c.QueryArgs().Peek("uploadId")),
	}
	if op.Operation == "CopyObject" {
		ev.CopySourceBucket, ev.CopySourceKey = parseAuditCopySource(string(c.GetHeader("x-amz-copy-source")))
	}
	return normalizeAuditEvent(ev)
}

func finalizeAuditEnvelopeEvent(c *app.RequestContext, ev audit.S3Event, start time.Time) audit.S3Event {
	ev.Status = int32(c.Response.StatusCode())
	if key, ok := c.Get(auditObjectKeyKey); ok {
		if s, ok := key.(string); ok && s != "" {
			ev.Key = s
		}
	}
	if ev.Status == 0 {
		ev.Status = int32(http.StatusOK)
	}
	if bytesOut, ok := c.Get(auditBytesOutKey); ok {
		if n, ok := bytesOut.(int64); ok {
			ev.BytesOut = n
		}
	} else {
		ev.BytesOut = int64(len(c.Response.Body()))
	}
	ev.LatencyMs = int32(time.Since(start).Milliseconds())

	// T51' §6: lift the Layer 1 policy decision (stashed by
	// rememberAuthzDecision) into the audit row. The middleware already sets
	// AuthStatus="deny" for 4xx responses; we only OVERWRITE that decision
	// here when the request was an anonymous Allow (anon_allow).
	if v, ok := c.Get(auditAuthzDecisionKey); ok {
		if dec, ok := v.(s3auth.Decision); ok {
			ev.MatchedPolicyID = dec.Detail.MatchedPolicyID
			ev.MatchedSID = dec.Detail.MatchedSID
			ev.AuthzLatencyUS = dec.AuthzLatencyUS
			ev.ConditionContext = dec.Detail.ConditionContext
			if anon, ok2 := c.Get(auditAuthzAnonAllowKey); ok2 {
				if b, ok3 := anon.(bool); ok3 && b && ev.Status < 400 {
					ev.AuthStatus = "anon_allow"
				}
			}
		}
	}
	return normalizeAuditEvent(ev)
}

// newAuditAuthFailureEvent constructs an audit row for SigV4 / authn rejection
// that aborts INSIDE authMiddleware — before auditEnvelopeMiddleware can call
// c.Next. The envelope finalizer (and rememberAuthzDecision) therefore never
// run on this path; the row is written directly via appendFinalizedAuditEvent.
// ErrReason carries the deny token; matched_policy_id / matched_sid /
// authz_latency_us / condition_context_json are intentionally zero (no Layer 1
// policy evaluation happened).
func (s *Server) newAuditAuthFailureEvent(
	ctx context.Context,
	c *app.RequestContext,
	bucket string,
	key string,
	requestID string,
	status int,
	reason string,
) audit.S3Event {
	now := time.Now()
	op := audit.ClassifyS3Operation(string(c.Method()), key != "", string(c.URI().QueryString()), toHTTPRequest(c).Header)
	return normalizeAuditEvent(audit.S3Event{
		Ts:          now.UnixMicro(),
		EventID:     uuid.NewString(),
		NodeID:      s.auditNodeID,
		RequestID:   requestID,
		SAID:        iam.PrincipalFromContext(ctx),
		SourceIP:    s.authoritativeClientIP(c),
		UserAgent:   auditString16(string(c.UserAgent())),
		Method:      auditString8(string(c.Method())),
		Operation:   op.Operation,
		Bucket:      bucket,
		Key:         key,
		Subresource: op.Subresource,
		Status:      int32(status),
		AuthStatus:  "deny",
		ErrClass:    "AccessDenied",
		ErrReason:   reason,
		BytesIn:     int64(len(c.Request.Body())),
	})
}
