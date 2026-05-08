package s3auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPhaseConstants(t *testing.T) {
	assert.Equal(t, Phase(0), PhasePreLoad)
	assert.Equal(t, Phase(1), PhasePostLoad)
}

func TestDecision_ZeroValueDeny(t *testing.T) {
	var d Decision
	assert.False(t, d.Allow)
	assert.Empty(t, d.Layer)
	assert.Empty(t, d.Reason)
}

func TestDependencyInterfaces_Compile(t *testing.T) {
	// Compile-time check: nil values of each interface type must be assignable.
	var (
		_ IAMStore          = (IAMStore)(nil)
		_ IAMChecker        = IAMChecker(nil)
		_ PolicyChecker     = (PolicyChecker)(nil)
		_ AuditEmitter      = (AuditEmitter)(nil)
		_ PrincipalResolver = PrincipalResolver(nil)
	)
	// Reference ctx so the import is needed.
	_ = context.Background()
}

type fakeAudit struct {
	allows []auditEntry
	denies []auditEntry
}

type auditEntry struct {
	saID, bucket, key string
	action            S3Action
	reason            string
}

func (f *fakeAudit) RecordAllow(_ context.Context, sa, b, k string, a S3Action) {
	f.allows = append(f.allows, auditEntry{sa, b, k, a, ""})
}
func (f *fakeAudit) RecordDeny(_ context.Context, sa, b, k string, a S3Action, r string) {
	f.denies = append(f.denies, auditEntry{sa, b, k, a, r})
}

type stubStore struct{ enabled bool }

func (s stubStore) AuthEnabled() bool { return s.enabled }

type stubPolicy struct{ allow bool }

func (p stubPolicy) Allow(_ context.Context, _ PermCheckInput) bool { return p.allow }

func basicInput(action S3Action) PermCheckInput {
	return PermCheckInput{
		Principal: Principal{AccessKey: "AK"},
		Resource:  ResourceRef{Bucket: "b", Key: "k"},
		Action:    action,
	}
}

func TestRequestAuthorizer_PreLoad_AuthDisabled_AllowsViaPolicy(t *testing.T) {
	audit := &fakeAudit{}
	r := NewRequestAuthorizer(
		stubStore{enabled: false},
		func(_, _ string, _ S3Action) bool { return false }, // would deny if consulted
		stubPolicy{allow: true},
		audit,
		func(_ context.Context) string { return "" },
	)
	d := r.Decide(context.Background(), basicInput(GetObject), PhasePreLoad)
	assert.True(t, d.Allow)
	assert.Equal(t, "anonymous_pass", d.Layer)
	assert.Empty(t, audit.allows, "no audit when auth disabled")
	assert.Empty(t, audit.denies)
}

func TestRequestAuthorizer_PreLoad_AuthEnabled_NoGrant_Denies(t *testing.T) {
	audit := &fakeAudit{}
	r := NewRequestAuthorizer(
		stubStore{enabled: true},
		func(_, _ string, _ S3Action) bool { return false },
		stubPolicy{allow: true},
		audit,
		func(_ context.Context) string { return "sa-1" },
	)
	d := r.Decide(context.Background(), basicInput(GetObject), PhasePreLoad)
	assert.False(t, d.Allow)
	assert.Equal(t, "iam_grant", d.Layer)
	assert.Equal(t, "no_grant", d.Reason)
	if assert.Len(t, audit.denies, 1) {
		assert.Equal(t, "sa-1", audit.denies[0].saID)
		assert.Equal(t, "no_grant", audit.denies[0].reason)
	}
	assert.Empty(t, audit.allows)
}

func TestRequestAuthorizer_PreLoad_AuthEnabled_GrantOnly_Allows(t *testing.T) {
	audit := &fakeAudit{}
	r := NewRequestAuthorizer(
		stubStore{enabled: true},
		func(saID, bucket string, a S3Action) bool {
			return saID == "sa-1" && bucket == "b" && a == GetObject
		},
		stubPolicy{allow: true},
		audit,
		func(_ context.Context) string { return "sa-1" },
	)
	d := r.Decide(context.Background(), basicInput(GetObject), PhasePreLoad)
	assert.True(t, d.Allow)
	assert.Equal(t, "iam_grant", d.Layer)
	assert.Empty(t, d.Reason)
	if assert.Len(t, audit.allows, 1) {
		assert.Equal(t, "sa-1", audit.allows[0].saID)
	}
	assert.Empty(t, audit.denies)
}
