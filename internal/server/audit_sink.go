package server

import (
	"context"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/audit"
)

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
