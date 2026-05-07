package iam

import (
	"context"
	"testing"
)

type fakeProposer struct {
	calls []string
}

func (f *fakeProposer) ProposeSACreate(ctx context.Context, sa ServiceAccount) error {
	f.calls = append(f.calls, "SACreate:"+sa.ID)
	return nil
}
func (f *fakeProposer) ProposeSADelete(ctx context.Context, saID string) error {
	f.calls = append(f.calls, "SADelete:"+saID)
	return nil
}
func (f *fakeProposer) ProposeKeyCreate(ctx context.Context, k AccessKey) error {
	f.calls = append(f.calls, "KeyCreate:"+k.AccessKey)
	return nil
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
	return nil
}
func (f *fakeProposer) ProposeAuthEnable(ctx context.Context) error {
	f.calls = append(f.calls, "AuthEnable")
	return nil
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
	s.applySACreate(ServiceAccount{ID: "sa-existing"})
	p := &fakeProposer{}
	if err := Bootstrap(context.Background(), s, p, "AKBOOT", "secret", true, newTestEncryptor(t)); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if len(p.calls) != 0 {
		t.Fatalf("non-empty IAM must not propose; got %v", p.calls)
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
