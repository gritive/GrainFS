package iam

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/s3auth"
)

// AuditStatus is the outcome of an authz decision.
type AuditStatus uint8

const (
	AuditStatusAllow     AuditStatus = 1
	AuditStatusDeny      AuditStatus = 2
	AuditStatusAnonAllow AuditStatus = 3
)

// String renders the status as a stable lowercase token suitable for
// structured logging and downstream filtering.
func (s AuditStatus) String() string {
	switch s {
	case AuditStatusAllow:
		return "allow"
	case AuditStatusDeny:
		return "deny"
	case AuditStatusAnonAllow:
		return "anon_allow"
	default:
		return "unknown"
	}
}

// AuditEvent is a single IAM authz decision record. Fields are the
// minimum needed to answer "who did what to which resource and was it
// allowed". Future fields (request_id, source_ip, region) can be added
// without breaking emitters.
type AuditEvent struct {
	Timestamp time.Time
	SAID      string // service account id; "(anonymous)" if empty
	Bucket    string
	Key       string
	Action    s3auth.S3Action
	Status    AuditStatus
	Reason    string // populated for deny events; empty for allow

	// Policy decision metadata (T51' §6). Optional; zero values mean
	// "not measured" / "no policy matched" / "no condition context".
	MatchedPolicyID  string
	MatchedSID       string
	AuthzLatencyUS   int32
	ConditionContext map[string]string
}

// AuditEmitter is the sink interface for AuditLogger. Implementations are
// best-effort: errors are returned for the caller to log/swallow but must
// not block or fail a request.
type AuditEmitter interface {
	Emit(ctx context.Context, ev AuditEvent) error
}

// AuditLogger emits IAM authz decisions to a configured AuditEmitter.
// nil-safe: a nil receiver is a silent no-op (used in unit tests where
// audit is irrelevant). All methods swallow emitter errors to avoid
// breaking the request path; failures are logged at warn level.
type AuditLogger struct {
	em AuditEmitter
}

// NewAuditLogger returns an AuditLogger that dispatches to em. Pass
// NewLogAuditEmitter() for the production zerolog sink, or a custom
// emitter for tests / future incident-store integration.
func NewAuditLogger(em AuditEmitter) *AuditLogger { return &AuditLogger{em: em} }

// AuditDetails carries the optional policy-decision metadata threaded from
// the authorizer's EvalResult. Type-aliased to s3auth.AuditAllowDetails so
// *AuditLogger satisfies s3auth.AuditEmitterDetailed at runtime — the two
// types are LITERALLY the same after Go's type-alias rule, not just
// structurally identical. A compile-time guard lives in
// internal/server/audit_sink.go (T51' B1 review fix).
type AuditDetails = s3auth.AuditAllowDetails

// RecordAllow emits an allow decision. Thin wrapper around
// RecordAllowDetailed for callers that don't have policy decision details.
func (a *AuditLogger) RecordAllow(ctx context.Context, saID, bucket, key string, action s3auth.S3Action) {
	a.emit(ctx, saID, bucket, key, action, AuditStatusAllow, "", AuditDetails{})
}

// RecordAllowDetailed emits an allow decision with policy decision metadata.
// Used by RequestAuthorizer.Decide for T51' §6.
func (a *AuditLogger) RecordAllowDetailed(ctx context.Context, saID, bucket, key string, action s3auth.S3Action, d AuditDetails) {
	a.emit(ctx, saID, bucket, key, action, AuditStatusAllow, "", d)
}

// RecordDeny emits a deny decision with the given reason (e.g., "no_grant",
// "policy_deny"). Reason is a short stable token for filtering, not free
// text.
func (a *AuditLogger) RecordDeny(ctx context.Context, saID, bucket, key string, action s3auth.S3Action, reason string) {
	a.emit(ctx, saID, bucket, key, action, AuditStatusDeny, reason, AuditDetails{})
}

// RecordDenyDetailed emits a deny decision with policy decision metadata.
// Used by RequestAuthorizer.Decide for T51' §6.
func (a *AuditLogger) RecordDenyDetailed(ctx context.Context, saID, bucket, key string, action s3auth.S3Action, reason string, d AuditDetails) {
	a.emit(ctx, saID, bucket, key, action, AuditStatusDeny, reason, d)
}

// RecordAnonAllow emits an allow decision for an anonymous request that was
// permitted by an iam.anon-enabled config or the default bucket's implicit
// anon policy. The status separates it from authenticated allows so audit
// consumers can filter on Phase 0 traffic.
func (a *AuditLogger) RecordAnonAllow(ctx context.Context, bucket, key string, action s3auth.S3Action, d AuditDetails) {
	a.emit(ctx, "", bucket, key, action, AuditStatusAnonAllow, "", d)
}

func (a *AuditLogger) emit(ctx context.Context, saID, bucket, key string, action s3auth.S3Action, status AuditStatus, reason string, d AuditDetails) {
	if a == nil || a.em == nil {
		return
	}
	if saID == "" {
		saID = audit.AnonSAID
	}
	ev := AuditEvent{
		Timestamp:        time.Now().UTC(),
		SAID:             saID,
		Bucket:           bucket,
		Key:              key,
		Action:           action,
		Status:           status,
		Reason:           reason,
		MatchedPolicyID:  d.MatchedPolicyID,
		MatchedSID:       d.MatchedSID,
		AuthzLatencyUS:   d.AuthzLatencyUS,
		ConditionContext: d.ConditionContext,
	}
	if err := a.em.Emit(ctx, ev); err != nil {
		log.Warn().Err(err).Str("sa_id", saID).Msg("iam audit emit failed")
	}
}

// logAuditEmitter writes audit events as structured zerolog lines on the
// "iam_audit" namespace. Production default.
type logAuditEmitter struct{}

// NewLogAuditEmitter returns the zerolog-backed emitter. Output goes
// through the project's standard logger, so it routes to the same
// destinations (stdout, file, SSE hub) as other server logs.
func NewLogAuditEmitter() AuditEmitter { return logAuditEmitter{} }

func (logAuditEmitter) Emit(_ context.Context, ev AuditEvent) error {
	evt := log.Debug()
	if ev.Status == AuditStatusDeny {
		evt = log.Info()
	}
	evt.
		Str("event", "iam_audit").
		Str("sa_id", ev.SAID).
		Str("bucket", ev.Bucket).
		Str("key", ev.Key).
		Uint16("action", uint16(ev.Action)).
		Str("status", ev.Status.String()).
		Str("reason", ev.Reason).
		Str("matched_policy_id", ev.MatchedPolicyID).
		Str("matched_sid", ev.MatchedSID).
		Int32("authz_latency_us", ev.AuthzLatencyUS).
		Time("ts", ev.Timestamp).
		Msg("iam.authz")
	return nil
}
