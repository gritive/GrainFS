package iam

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// stubPostureChecker is a minimal PostureChecker for unit tests. Returns err
// from CheckAnonOff on every call and records whether it was invoked.
type stubPostureChecker struct {
	err    error
	called int
}

func (s *stubPostureChecker) CheckAnonOff(_ context.Context) error {
	s.called++
	return s.err
}

// TestAdminAPI_CreateSA_PostureCheck_EmptyStore_Rejects covers F#26-tls-posture:
// when the store is empty (first SA create) AND the configured PostureChecker
// reports a bad posture, CreateSA must return a "precondition" admin error
// containing the remediation hint and must NOT call the proposer.
func TestAdminAPI_CreateSA_PostureCheck_EmptyStore_Rejects(t *testing.T) {
	store := NewStore()
	p := newFakeProposer()
	api := NewAdminAPI(store, p, newTestEncryptor(t))
	postureErr := errors.New("auth required + no TLS cert + no trusted proxy. Place cert at /x, OR set GRAINFS_TLS_CERT/KEY, OR `grainfs config set trusted-proxy.cidr <cidr>`")
	pc := &stubPostureChecker{err: postureErr}
	api.SetPostureChecker(pc)

	_, err := api.CreateSA(context.Background(), SACreateRequest{Name: "admin"})
	if err == nil {
		t.Fatal("CreateSA: expected error, got nil")
	}
	var ae *adminapi.Error
	if !errors.As(err, &ae) {
		t.Fatalf("CreateSA: error type = %T, want *adminapi.Error", err)
	}
	if ae.Code != "precondition" {
		t.Errorf("Code = %q, want %q", ae.Code, "precondition")
	}
	if !strings.Contains(ae.Message, "trusted-proxy.cidr") {
		t.Errorf("Message = %q, want remediation hint mentioning trusted-proxy.cidr", ae.Message)
	}
	if pc.called != 1 {
		t.Errorf("PostureChecker.CheckAnonOff called %d times, want 1", pc.called)
	}
	// Proposer must not have been called — the pre-check fired before propose.
	if len(p.calls) != 0 {
		t.Errorf("proposer.calls = %v, want empty (pre-check should short-circuit)", p.calls)
	}
}

// TestAdminAPI_CreateSA_PostureCheck_EmptyStore_OK_Allows: empty store + posture
// OK → CreateSA succeeds, proposer is called.
func TestAdminAPI_CreateSA_PostureCheck_EmptyStore_OK_Allows(t *testing.T) {
	store := NewStore()
	p := newFakeProposer()
	api := NewAdminAPI(store, p, newTestEncryptor(t))
	pc := &stubPostureChecker{err: nil}
	api.SetPostureChecker(pc)

	resp, err := api.CreateSA(context.Background(), SACreateRequest{Name: "admin"})
	if err != nil {
		t.Fatalf("CreateSA: %v", err)
	}
	if resp.SAID == "" || resp.AccessKey == "" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if pc.called != 1 {
		t.Errorf("PostureChecker.CheckAnonOff called %d times, want 1", pc.called)
	}
	// SACreate + KeyCreate should both have been proposed.
	if len(p.calls) != 2 {
		t.Errorf("proposer.calls = %v, want 2 (SACreate + KeyCreate)", p.calls)
	}
}

// TestAdminAPI_CreateSA_PostureCheck_NonEmptyStore_Skipped: when an SA already
// exists, the pre-check is skipped even if posture is bad. Trust boundary was
// already crossed; only the FIRST SA flips anon → no need to gate subsequent
// creates.
func TestAdminAPI_CreateSA_PostureCheck_NonEmptyStore_Skipped(t *testing.T) {
	store := NewStore()
	store.applySACreate(ServiceAccount{ID: "sa-existing", Name: "preexisting"})
	p := newFakeProposer()
	api := NewAdminAPI(store, p, newTestEncryptor(t))
	pc := &stubPostureChecker{err: errors.New("bad posture")}
	api.SetPostureChecker(pc)

	resp, err := api.CreateSA(context.Background(), SACreateRequest{Name: "second"})
	if err != nil {
		t.Fatalf("CreateSA: unexpected error %v", err)
	}
	if resp.SAID == "" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if pc.called != 0 {
		t.Errorf("PostureChecker.CheckAnonOff called %d times, want 0 (skipped for non-empty store)", pc.called)
	}
}

// TestAdminAPI_CreateSA_NilPostureChecker_BackwardCompat: AdminAPI constructed
// without a PostureChecker preserves legacy behavior — no pre-check, CreateSA
// succeeds regardless of TLS posture.
func TestAdminAPI_CreateSA_NilPostureChecker_BackwardCompat(t *testing.T) {
	store := NewStore()
	p := newFakeProposer()
	api := NewAdminAPI(store, p, newTestEncryptor(t))
	// No SetPostureChecker call.

	resp, err := api.CreateSA(context.Background(), SACreateRequest{Name: "admin"})
	if err != nil {
		t.Fatalf("CreateSA: %v", err)
	}
	if resp.SAID == "" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}
