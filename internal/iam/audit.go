package iam

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/s3auth"
)

// AuditStatus is the outcome of an authz decision.
type AuditStatus uint8

const (
	AuditStatusAllow AuditStatus = 1
	AuditStatusDeny  AuditStatus = 2
)

// String renders the status as a stable lowercase token suitable for
// structured logging and downstream filtering.
func (s AuditStatus) String() string {
	switch s {
	case AuditStatusAllow:
		return "allow"
	case AuditStatusDeny:
		return "deny"
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

// RecordAllow emits an allow decision.
func (a *AuditLogger) RecordAllow(ctx context.Context, saID, bucket, key string, action s3auth.S3Action) {
	a.emit(ctx, saID, bucket, key, action, AuditStatusAllow, "")
}

// RecordDeny emits a deny decision with the given reason (e.g., "no_grant",
// "policy_deny"). Reason is a short stable token for filtering, not free
// text.
func (a *AuditLogger) RecordDeny(ctx context.Context, saID, bucket, key string, action s3auth.S3Action, reason string) {
	a.emit(ctx, saID, bucket, key, action, AuditStatusDeny, reason)
}

func (a *AuditLogger) emit(ctx context.Context, saID, bucket, key string, action s3auth.S3Action, status AuditStatus, reason string) {
	if a == nil || a.em == nil {
		return
	}
	if saID == "" {
		saID = "(anonymous)"
	}
	ev := AuditEvent{
		Timestamp: time.Now().UTC(),
		SAID:      saID,
		Bucket:    bucket,
		Key:       key,
		Action:    action,
		Status:    status,
		Reason:    reason,
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
		Time("ts", ev.Timestamp).
		Msg("iam.authz")
	return nil
}
