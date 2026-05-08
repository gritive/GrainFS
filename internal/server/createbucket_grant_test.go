package server

import (
	"context"
	"errors"
	"testing"

	"github.com/gritive/GrainFS/internal/iam"
)

// mockIAMProposer captures the last ProposeGrantPut call. All other
// Proposer methods return nil unless ErrOnGrant is set.
type mockIAMProposer struct {
	calls      []iam.Grant
	errOnGrant error
}

func (m *mockIAMProposer) ProposeSACreate(_ context.Context, _ iam.ServiceAccount) error {
	return nil
}
func (m *mockIAMProposer) ProposeSADelete(_ context.Context, _ string) error { return nil }
func (m *mockIAMProposer) ProposeKeyCreate(_ context.Context, _ iam.AccessKey) error {
	return nil
}
func (m *mockIAMProposer) ProposeKeyRevoke(_ context.Context, _ string) error { return nil }
func (m *mockIAMProposer) ProposeGrantPut(_ context.Context, g iam.Grant) error {
	m.calls = append(m.calls, g)
	return m.errOnGrant
}
func (m *mockIAMProposer) ProposeGrantDelete(_ context.Context, _, _ string) error { return nil }
func (m *mockIAMProposer) ProposeGrantWildcardPut(_ context.Context, _ iam.Grant) error {
	return nil
}
func (m *mockIAMProposer) ProposeGrantWildcardDelete(_ context.Context, _ string) error {
	return nil
}
func (m *mockIAMProposer) ProposeAuthEnable(_ context.Context) error { return nil }

func TestIssueCreatorGrant(t *testing.T) {
	tests := []struct {
		name         string
		principal    string
		proposer     *mockIAMProposer // nil → s.iamProposer = nil
		wantNumCalls int
		wantSAID     string
		wantBucket   string
	}{
		{
			name:         "principal_present_proposer_wired",
			principal:    "sa-alice",
			proposer:     &mockIAMProposer{},
			wantNumCalls: 1,
			wantSAID:     "sa-alice",
			wantBucket:   "alice-bucket",
		},
		{
			name:         "principal_present_proposer_nil",
			principal:    "sa-alice",
			proposer:     nil,
			wantNumCalls: 0,
		},
		{
			name:         "anonymous_no_propose",
			principal:    "",
			proposer:     &mockIAMProposer{},
			wantNumCalls: 0,
		},
		{
			name:         "proposer_error_swallowed",
			principal:    "sa-alice",
			proposer:     &mockIAMProposer{errOnGrant: errors.New("raft down")},
			wantNumCalls: 1,
			wantSAID:     "sa-alice",
			wantBucket:   "alice-bucket",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{}
			if tc.proposer != nil {
				s.iamProposer = tc.proposer
			}
			ctx := context.Background()
			if tc.principal != "" {
				ctx = iam.WithPrincipal(ctx, tc.principal)
			}
			// Must not panic and must not return any error (best-effort).
			s.issueCreatorGrant(ctx, "alice-bucket")

			if tc.proposer == nil {
				return
			}
			if got := len(tc.proposer.calls); got != tc.wantNumCalls {
				t.Fatalf("ProposeGrantPut calls = %d, want %d", got, tc.wantNumCalls)
			}
			if tc.wantNumCalls == 0 {
				return
			}
			g := tc.proposer.calls[0]
			if g.SAID != tc.wantSAID {
				t.Errorf("Grant.SAID = %q, want %q", g.SAID, tc.wantSAID)
			}
			if g.Bucket != tc.wantBucket {
				t.Errorf("Grant.Bucket = %q, want %q", g.Bucket, tc.wantBucket)
			}
			if g.Role != iam.RoleAdmin {
				t.Errorf("Grant.Role = %v, want RoleAdmin", g.Role)
			}
			if g.CreatedBy != tc.wantSAID {
				t.Errorf("Grant.CreatedBy = %q, want %q", g.CreatedBy, tc.wantSAID)
			}
			if g.CreatedAt.IsZero() {
				t.Errorf("Grant.CreatedAt is zero")
			}
		})
	}
}
