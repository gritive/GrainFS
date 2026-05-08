package iam

import (
	"context"
	"testing"
)

type fakeProposer struct {
	calls          []string
	authEnableErr  error // if non-nil, ProposeAuthEnable returns this
	saCreateErr    error // if non-nil, ProposeSACreate returns this
	keyCreateErr   error // if non-nil, ProposeKeyCreate returns this
	wildcardPutErr error // if non-nil, ProposeGrantWildcardPut returns this
}

func newFakeProposer() *fakeProposer { return &fakeProposer{} }

func (f *fakeProposer) calledSADelete(saID string) bool {
	for _, c := range f.calls {
		if c == "SADelete:"+saID {
			return true
		}
	}
	return false
}

func (f *fakeProposer) calledKeyCreate(ak string) bool {
	for _, c := range f.calls {
		if c == "KeyCreate:"+ak {
			return true
		}
	}
	return false
}

func (f *fakeProposer) calledKeyRevoke(ak string) bool {
	for _, c := range f.calls {
		if c == "KeyRevoke:"+ak {
			return true
		}
	}
	return false
}

func (f *fakeProposer) calledGrantPut(saID, bucket string) bool {
	for _, c := range f.calls {
		if c == "GrantPut:"+saID+":"+bucket {
			return true
		}
	}
	return false
}

func (f *fakeProposer) calledGrantDelete(saID, bucket string) bool {
	for _, c := range f.calls {
		if c == "GrantDelete:"+saID+":"+bucket {
			return true
		}
	}
	return false
}

func (f *fakeProposer) calledGrantWildcardDelete(saID string) bool {
	for _, c := range f.calls {
		if c == "GrantWildcardDelete:"+saID {
			return true
		}
	}
	return false
}

func (f *fakeProposer) ProposeSACreate(ctx context.Context, sa ServiceAccount) error {
	f.calls = append(f.calls, "SACreate:"+sa.ID)
	return f.saCreateErr
}
func (f *fakeProposer) ProposeSADelete(ctx context.Context, saID string) error {
	f.calls = append(f.calls, "SADelete:"+saID)
	return nil
}
func (f *fakeProposer) ProposeKeyCreate(ctx context.Context, k AccessKey) error {
	f.calls = append(f.calls, "KeyCreate:"+k.AccessKey)
	return f.keyCreateErr
}
func (f *fakeProposer) ProposeKeyCreateScoped(ctx context.Context, k AccessKey) error {
	f.calls = append(f.calls, "KeyCreateScoped:"+k.AccessKey)
	return f.keyCreateErr
}
func (f *fakeProposer) ProposeKeyRevoke(ctx context.Context, accessKey string) error {
	f.calls = append(f.calls, "KeyRevoke:"+accessKey)
	return nil
}
func (f *fakeProposer) ProposeGrantPut(ctx context.Context, g Grant) error {
	f.calls = append(f.calls, "GrantPut:"+g.SAID+":"+g.Bucket)
	return nil
}
func (f *fakeProposer) ProposeGrantDelete(ctx context.Context, saID, bucket string) error {
	f.calls = append(f.calls, "GrantDelete:"+saID+":"+bucket)
	return nil
}
func (f *fakeProposer) ProposeGrantWildcardPut(ctx context.Context, g Grant) error {
	f.calls = append(f.calls, "GrantWildcard:"+g.SAID)
	return f.wildcardPutErr
}
func (f *fakeProposer) ProposeGrantWildcardDelete(ctx context.Context, saID string) error {
	f.calls = append(f.calls, "GrantWildcardDelete:"+saID)
	return nil
}
func (f *fakeProposer) ProposeAuthEnable(ctx context.Context) error {
	f.calls = append(f.calls, "AuthEnable")
	return f.authEnableErr
}

func TestBootstrap_NoFlag_NoOp(t *testing.T) {
	s := NewStore()
	p := &fakeProposer{}
	if err := Bootstrap(context.Background(), s, p, "", "", true /* isLeader */, newTestEncryptor(t)); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if len(p.calls) != 0 {
		t.Fatalf("expected no proposals, got %v", p.calls)
	}
}

func TestBootstrap_FlagPresentEmptyIAM_LeaderProposes(t *testing.T) {
	s := NewStore()
	p := &fakeProposer{}
	if err := Bootstrap(context.Background(), s, p, "AKBOOT", "secret", true, newTestEncryptor(t)); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	want := []string{
		"SACreate:" + DefaultSAID,
		"KeyCreate:AKBOOT",
		"GrantWildcard:" + DefaultSAID,
		"AuthEnable",
	}
	if !equalSlices(p.calls, want) {
		t.Fatalf("calls = %v, want %v", p.calls, want)
	}
}

func TestBootstrap_NotLeader_NoProposal(t *testing.T) {
	s := NewStore()
	p := &fakeProposer{}
	if err := Bootstrap(context.Background(), s, p, "AKBOOT", "secret", false, newTestEncryptor(t)); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if len(p.calls) != 0 {
		t.Fatalf("follower must not propose; got %v", p.calls)
	}
}

func TestBootstrap_IAMNotEmpty_NoProposal(t *testing.T) {
	s := NewStore()
	// Fake "complete" bootstrap state so the new bootstrapComplete() guard
	// short-circuits exactly like the pre-fix IsEmpty() did when a separate
	// SA was registered. Pre-fix took ANY SA; post-fix is stricter.
	s.applySACreate(ServiceAccount{ID: DefaultSAID})
	wrapped, _ := WrapSecret(newTestEncryptor(t), DefaultSAID, "x")
	s.applyKeyCreate(AccessKey{
		AccessKey: "AKBOOT", SAID: DefaultSAID, SecretKey: "x",
		SecretKeyEnc: wrapped, Status: KeyStatusActive,
	})
	s.applyGrantWildcardPut(Grant{SAID: DefaultSAID, Role: RoleAdmin})
	s.applyAuthEnable()
	p := &fakeProposer{}
	if err := Bootstrap(context.Background(), s, p, "AKBOOT", "secret", true, newTestEncryptor(t)); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if len(p.calls) != 0 {
		t.Fatalf("complete bootstrap must not propose; got %v", p.calls)
	}
}

// TestBootstrap_PartialState_Retries verifies the bootstrapComplete()
// completeness check — pre-fix used IsEmpty() and skipped the retry,
// leaving auth_enabled false on a partial-state cluster.
func TestBootstrap_PartialState_Retries(t *testing.T) {
	enc := newTestEncryptor(t)
	s := NewStore()
	// Simulate an interrupted previous Bootstrap: SA + key + wildcard
	// committed but AuthEnable never landed (auth_enabled still false).
	s.applySACreate(ServiceAccount{ID: DefaultSAID, Name: "default"})
	wrapped, _ := WrapSecret(enc, DefaultSAID, "secret")
	s.applyKeyCreate(AccessKey{
		AccessKey: "AKBOOT", SAID: DefaultSAID, SecretKey: "secret",
		SecretKeyEnc: wrapped, Status: KeyStatusActive,
	})
	s.applyGrantWildcardPut(Grant{SAID: DefaultSAID, Role: RoleAdmin})

	p := &fakeProposer{}
	if err := Bootstrap(context.Background(), s, p, "AKBOOT", "secret", true, enc); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	// Each ProposeXxx is idempotent on the apply path; what matters is that
	// AuthEnable fires (and the earlier ones are safe to re-issue).
	saw := false
	for _, c := range p.calls {
		if c == "AuthEnable" {
			saw = true
			break
		}
	}
	if !saw {
		t.Fatalf("AuthEnable must re-fire when sticky bit absent; calls=%v", p.calls)
	}
}

func TestBootstrap_HalfFlag_NoOp(t *testing.T) {
	s := NewStore()
	p := &fakeProposer{}
	// Only access-key set, secret missing — no bootstrap.
	if err := Bootstrap(context.Background(), s, p, "AKBOOT", "", true, newTestEncryptor(t)); err != nil {
		t.Fatalf("Bootstrap (ak only): %v", err)
	}
	if len(p.calls) != 0 {
		t.Fatalf("half-flag (ak only) proposed: %v", p.calls)
	}
	// Only secret set, access-key missing — no bootstrap.
	if err := Bootstrap(context.Background(), s, p, "", "secret", true, newTestEncryptor(t)); err != nil {
		t.Fatalf("Bootstrap (secret only): %v", err)
	}
	if len(p.calls) != 0 {
		t.Fatalf("half-flag (secret only) proposed: %v", p.calls)
	}
}

func TestNewUUIDv7_NotEmpty(t *testing.T) {
	id := NewUUIDv7()
	if id == "" {
		t.Fatal("NewUUIDv7 returned empty string")
	}
	// Two consecutive calls must produce distinct IDs.
	if id == NewUUIDv7() {
		t.Fatal("two consecutive NewUUIDv7 calls returned identical IDs")
	}
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
