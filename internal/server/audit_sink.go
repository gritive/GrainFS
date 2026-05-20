package server

import (
	"context"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/s3auth"
)

// Compile-time guard: *iam.AuditLogger MUST satisfy
// s3auth.AuditEmitterDetailed so RequestAuthorizer.Decide's runtime type
// assertion succeeds and the detailed audit emit path actually fires in
// production. Both methods take s3auth.AuditAllowDetails (with
// iam.AuditDetails as a Go type alias for the same type). T51' B1 review.
//
// Audit emission has two INDEPENDENT sinks:
//
//  1. AuditEmitter (iam.AuditLogger): structured zerolog line "iam.authz";
//     immediate, per-call. RecordAllowDetailed/RecordDenyDetailed/
//     RecordAnonAllow carry matched_policy_id / matched_sid /
//     authz_latency_us / condition_context.
//  2. audit.s3 Iceberg table (auditSink below): built from the request context
//     via rememberAuthzDecision → auditAuthzDecisionKey →
//     finalizeAuditEnvelopeEvent; flushed via outbox at request end.
//
// Both are fed from the same Decision; if you change the Decision shape,
// update BOTH paths.
var _ s3auth.AuditEmitterDetailed = (*iam.AuditLogger)(nil)

type auditSink struct {
	outbox  *audit.Outbox
	emitter *audit.Emitter
}

func (s *Server) auditSink() auditSink {
	return auditSink{
		outbox:  s.auditOutbox,
		emitter: s.auditEmitter,
	}
}

func (s *Server) auditSinkConfigured() bool {
	return s.auditSink().configured()
}

func (sink auditSink) configured() bool {
	return sink.outbox != nil || sink.emitter != nil
}

func (sink auditSink) usesOutbox() bool {
	return sink.outbox != nil
}

func (s *Server) appendAuditAttempt(ctx context.Context, ev audit.S3Event) error {
	return s.auditSink().appendAttempt(ctx, ev)
}

func (sink auditSink) appendAttempt(ctx context.Context, ev audit.S3Event) error {
	if !sink.usesOutbox() {
		return nil
	}
	return sink.outbox.AppendAttempt(ctx, ev)
}

func (s *Server) finalizeAuditEvent(ctx context.Context, ev audit.S3Event) {
	s.auditSink().finalize(ctx, ev)
}

func (sink auditSink) finalize(ctx context.Context, ev audit.S3Event) {
	if sink.usesOutbox() {
		if err := sink.outbox.Finalize(ctx, ev); err != nil {
			log.Error().Err(err).Str("event_id", ev.EventID).Msg("audit envelope: finalize failed")
		}
		return
	}
	sink.emitter.EmitS3(ev)
}

func (s *Server) appendFinalizedAuditEvent(ctx context.Context, ev audit.S3Event) {
	s.auditSink().appendFinalized(ctx, ev)
}

func (sink auditSink) appendFinalized(ctx context.Context, ev audit.S3Event) {
	if sink.usesOutbox() {
		if err := sink.outbox.AppendFinalized(ctx, ev); err != nil {
			log.Error().Err(err).Str("event_id", ev.EventID).Msg("audit envelope: auth failure record failed")
		}
		return
	}
	sink.emitter.EmitS3(ev)
}

func (s *Server) auditHealthSnapshot(ctx context.Context) auditHealthResponse {
	return s.auditSink().healthSnapshot(ctx)
}

func (sink auditSink) healthSnapshot(ctx context.Context) auditHealthResponse {
	stats, err := sink.outbox.Stats(ctx)
	if err != nil {
		return auditHealthResponse{
			Enabled:        true,
			GuaranteeState: "critical",
		}
	}
	return auditHealthResponse{
		Enabled:         true,
		GuaranteeState:  "ok",
		OutboxBacklog:   stats.Backlog,
		OldestPendingUS: stats.OldestPendingUS,
	}
}
