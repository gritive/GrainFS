package iam

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

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

func TestAuditLogger_RecordAllowDetailed_CarriesPolicyMetadata(t *testing.T) {
	em := &fakeEmitter{}
	a := NewAuditLogger(em)
	a.RecordAllowDetailed(context.Background(), "sa-1", "b", "k", s3auth.GetObject, AuditDetails{
		MatchedPolicyID:  "readonly",
		MatchedSID:       "AllowGet",
		AuthzLatencyUS:   42,
		ConditionContext: map[string]string{"aws:SourceIp": "10.0.0.1"},
	})
	got := em.snapshot()
	if len(got) != 1 {
		t.Fatalf("got %d events", len(got))
	}
	if got[0].Status != AuditStatusAllow {
		t.Fatalf("status = %v want Allow", got[0].Status)
	}
	if got[0].MatchedPolicyID != "readonly" || got[0].MatchedSID != "AllowGet" {
		t.Fatalf("policy/sid mismatch: %+v", got[0])
	}
	if got[0].AuthzLatencyUS != 42 {
		t.Fatalf("latency = %d want 42", got[0].AuthzLatencyUS)
	}
	if got[0].ConditionContext["aws:SourceIp"] != "10.0.0.1" {
		t.Fatalf("condition context = %v", got[0].ConditionContext)
	}
}

func TestAuditLogger_RecordDenyDetailed_CarriesPolicyMetadata(t *testing.T) {
	em := &fakeEmitter{}
	a := NewAuditLogger(em)
	a.RecordDenyDetailed(context.Background(), "sa-2", "b", "k", s3auth.PutObject, "policy_deny", AuditDetails{
		MatchedPolicyID: "deny-puts",
		MatchedSID:      "DenyPut",
		AuthzLatencyUS:  77,
	})
	got := em.snapshot()
	if len(got) != 1 {
		t.Fatalf("got %d events", len(got))
	}
	if got[0].Status != AuditStatusDeny || got[0].Reason != "policy_deny" {
		t.Fatalf("status/reason mismatch: %+v", got[0])
	}
	if got[0].MatchedPolicyID != "deny-puts" || got[0].MatchedSID != "DenyPut" || got[0].AuthzLatencyUS != 77 {
		t.Fatalf("policy/sid/latency mismatch: %+v", got[0])
	}
}

func TestAuditLogger_RecordAnonAllow_StatusAndAnonymousSA(t *testing.T) {
	em := &fakeEmitter{}
	a := NewAuditLogger(em)
	a.RecordAnonAllow(context.Background(), "default", "k", s3auth.GetObject, AuditDetails{
		AuthzLatencyUS: 9,
	})
	got := em.snapshot()
	if len(got) != 1 {
		t.Fatalf("got %d events", len(got))
	}
	if got[0].Status != AuditStatusAnonAllow {
		t.Fatalf("status = %v want AnonAllow", got[0].Status)
	}
	if got[0].SAID != "(anonymous)" {
		t.Fatalf("SAID = %q want (anonymous)", got[0].SAID)
	}
	if got[0].AuthzLatencyUS != 9 {
		t.Fatalf("latency = %d", got[0].AuthzLatencyUS)
	}
}

func TestAuditStatus_String_AnonAllow(t *testing.T) {
	if AuditStatusAnonAllow.String() != "anon_allow" {
		t.Fatalf("AuditStatusAnonAllow.String() = %q", AuditStatusAnonAllow.String())
	}
}

func TestLogAuditEmitter_DefaultInfoLevelSuppressesAllowButKeepsDeny(t *testing.T) {
	var buf bytes.Buffer
	prev := log.Logger
	log.Logger = zerolog.New(&buf).Level(zerolog.InfoLevel)
	t.Cleanup(func() { log.Logger = prev })

	em := NewLogAuditEmitter()
	if err := em.Emit(context.Background(), AuditEvent{
		SAID: "sa-1", Bucket: "b", Key: "allowed",
		Action: s3auth.PutObject, Status: AuditStatusAllow,
	}); err != nil {
		t.Fatalf("allow emit returned error: %v", err)
	}
	if strings.Contains(buf.String(), "iam.authz") {
		t.Fatalf("allow audit log should be debug-only at default info level, got %q", buf.String())
	}

	if err := em.Emit(context.Background(), AuditEvent{
		SAID: "sa-1", Bucket: "b", Key: "denied",
		Action: s3auth.PutObject, Status: AuditStatusDeny, Reason: "no_grant",
	}); err != nil {
		t.Fatalf("deny emit returned error: %v", err)
	}
	if !strings.Contains(buf.String(), "iam.authz") || !strings.Contains(buf.String(), `"status":"deny"`) {
		t.Fatalf("deny audit log should remain visible at info level, got %q", buf.String())
	}
}
