package iam

import (
	"context"
	"sync"
	"testing"

	"github.com/gritive/GrainFS/internal/s3auth"
)

type fakeEmitter struct {
	mu     sync.Mutex
	events []AuditEvent
}

func (f *fakeEmitter) Emit(ctx context.Context, ev AuditEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, ev)
	return nil
}

func (f *fakeEmitter) snapshot() []AuditEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]AuditEvent, len(f.events))
	copy(out, f.events)
	return out
}

func TestAuditLogger_RecordAllow(t *testing.T) {
	em := &fakeEmitter{}
	a := NewAuditLogger(em)
	ctx := context.Background()

	a.RecordAllow(ctx, "sa-1", "logs", "events.json", s3auth.PutObject)

	got := em.snapshot()
	if len(got) != 1 {
		t.Fatalf("got %d events, want 1", len(got))
	}
	if got[0].SAID != "sa-1" || got[0].Bucket != "logs" || got[0].Key != "events.json" ||
		got[0].Action != s3auth.PutObject || got[0].Status != AuditStatusAllow {
		t.Errorf("event mismatch: %+v", got[0])
	}
	if got[0].Reason != "" {
		t.Errorf("allow event should have empty reason, got %q", got[0].Reason)
	}
	if got[0].Timestamp.IsZero() {
		t.Error("timestamp should be populated")
	}
}

func TestAuditLogger_RecordDeny_WithReason(t *testing.T) {
	em := &fakeEmitter{}
	a := NewAuditLogger(em)
	ctx := context.Background()

	a.RecordDeny(ctx, "sa-2", "secrets", "k", s3auth.GetObject, "no_grant")

	got := em.snapshot()
	if len(got) != 1 {
		t.Fatalf("got %d events, want 1", len(got))
	}
	if got[0].Status != AuditStatusDeny {
		t.Errorf("Status = %v, want %v", got[0].Status, AuditStatusDeny)
	}
	if got[0].Reason != "no_grant" {
		t.Errorf("Reason = %q, want no_grant", got[0].Reason)
	}
}

func TestAuditLogger_AnonymousPrincipal(t *testing.T) {
	em := &fakeEmitter{}
	a := NewAuditLogger(em)
	a.RecordAllow(context.Background(), "", "any", "k", s3auth.GetObject)

	got := em.snapshot()
	if len(got) != 1 {
		t.Fatalf("got %d events, want 1", len(got))
	}
	if got[0].SAID != "(anonymous)" {
		t.Errorf("empty SAID should normalize to (anonymous), got %q", got[0].SAID)
	}
}

func TestAuditLogger_NilLoggerOrEmitter_NoPanic(t *testing.T) {
	// nil AuditLogger: methods are no-ops
	var nilLogger *AuditLogger
	nilLogger.RecordAllow(context.Background(), "sa", "b", "k", s3auth.GetObject)
	nilLogger.RecordDeny(context.Background(), "sa", "b", "k", s3auth.GetObject, "x")

	// AuditLogger with nil emitter: methods are no-ops
	a := &AuditLogger{}
	a.RecordAllow(context.Background(), "sa", "b", "k", s3auth.GetObject)
	a.RecordDeny(context.Background(), "sa", "b", "k", s3auth.GetObject, "x")
}

func TestLogAuditEmitter_DoesNotError(t *testing.T) {
	// The zerolog-based emitter should never return an error in normal use.
	em := NewLogAuditEmitter()
	if err := em.Emit(context.Background(), AuditEvent{
		SAID: "sa-1", Bucket: "b", Key: "k",
		Action: s3auth.GetObject, Status: AuditStatusAllow,
	}); err != nil {
		t.Errorf("LogAuditEmitter.Emit returned %v, want nil", err)
	}
}
