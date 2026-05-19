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
		func(_, _, _ string, _ S3Action) bool { return false }, // would deny if consulted
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
		func(_, _, _ string, _ S3Action) bool { return false },
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
		func(saID, bucket, _ string, a S3Action) bool {
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

func TestRequestAuthorizer_PreLoad_PolicyDenies(t *testing.T) {
	audit := &fakeAudit{}
	r := NewRequestAuthorizer(
		stubStore{enabled: true},
		func(_, _, _ string, _ S3Action) bool { return true },
		stubPolicy{allow: false},
		audit,
		func(_ context.Context) string { return "sa-1" },
	)
	d := r.Decide(context.Background(), basicInput(GetObject), PhasePreLoad)
	assert.False(t, d.Allow)
	assert.Equal(t, "bucket_policy", d.Layer)
	assert.Equal(t, "policy_deny", d.Reason)
	if assert.Len(t, audit.denies, 1) {
		assert.Equal(t, "policy_deny", audit.denies[0].reason)
	}
}

func TestRequestAuthorizer_PreLoad_PolicyExempt_BucketPolicyCRUD(t *testing.T) {
	audit := &fakeAudit{}
	r := NewRequestAuthorizer(
		stubStore{enabled: true},
		func(_, _, _ string, _ S3Action) bool { return true }, // IAM allows
		stubPolicy{allow: false},                              // policy would deny
		audit,
		func(_ context.Context) string { return "sa-1" },
	)
	for _, action := range []S3Action{GetBucketPolicy, PutBucketPolicy, DeleteBucketPolicy} {
		d := r.Decide(context.Background(), basicInput(action), PhasePreLoad)
		assert.True(t, d.Allow, "action=%v must bypass policy when IAM allows", action)
	}
	assert.Empty(t, audit.denies, "no policy denies for bucket-policy CRUD")
}

func TestRequestAuthorizer_PreLoad_AnonymousMode_PolicyDenies(t *testing.T) {
	audit := &fakeAudit{}
	r := NewRequestAuthorizer(
		stubStore{enabled: false}, // auth disabled
		func(_, _, _ string, _ S3Action) bool { return false },
		stubPolicy{allow: false},
		audit,
		func(_ context.Context) string { return "" },
	)
	d := r.Decide(context.Background(), basicInput(GetObject), PhasePreLoad)
	assert.False(t, d.Allow)
	assert.Equal(t, "bucket_policy", d.Layer)
	assert.Equal(t, "policy_deny", d.Reason)
}

func TestRequestAuthorizer_PostLoad_ACLMatrix(t *testing.T) {
	cases := []struct {
		name       string
		acl        ACLGrant
		accessKey  string
		action     S3Action
		wantAllow  bool
		wantLayer  string
		wantReason string
	}{
		// Public-read-write: every action allowed including anonymous writes.
		{"prw_anon_get", ACLPublicReadWrite, "", GetObject, true, "object_acl_public_read_write", ""},
		{"prw_anon_put", ACLPublicReadWrite, "", PutObject, true, "object_acl_public_read_write", ""},
		{"prw_auth_delete", ACLPublicReadWrite, "AK", DeleteObject, true, "object_acl_public_read_write", ""},

		// Public-read: anonymous reads only.
		{"pr_anon_get", ACLPublicRead, "", GetObject, true, "object_acl_public_read", ""},
		{"pr_anon_head", ACLPublicRead, "", HeadObject, true, "object_acl_public_read", ""},
		{"pr_anon_put_denied", ACLPublicRead, "", PutObject, false, "object_acl", "acl_anonymous_denied"},

		// Private: authenticated only.
		{"private_anon_get_denied", ACLPrivate, "", GetObject, false, "object_acl", "acl_anonymous_denied"},
		{"private_auth_get", ACLPrivate, "AK", GetObject, true, "object_acl_authenticated", ""},
		{"private_auth_put", ACLPrivate, "AK", PutObject, true, "object_acl_authenticated", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			audit := &fakeAudit{}
			r := NewRequestAuthorizer(
				stubStore{enabled: false}, // skip Layer 1 to isolate Layer 3
				func(_, _, _ string, _ S3Action) bool { return true },
				stubPolicy{allow: true},
				audit,
				func(_ context.Context) string { return "" },
			)
			in := PermCheckInput{
				Principal: Principal{AccessKey: tc.accessKey},
				Resource:  ResourceRef{Bucket: "b", Key: "k"},
				Action:    tc.action,
				ObjectACL: tc.acl,
			}
			d := r.Decide(context.Background(), in, PhasePostLoad)
			assert.Equal(t, tc.wantAllow, d.Allow)
			assert.Equal(t, tc.wantLayer, d.Layer)
			assert.Equal(t, tc.wantReason, d.Reason)
			if !tc.wantAllow {
				if assert.Len(t, audit.denies, 1) {
					assert.Equal(t, tc.wantReason, audit.denies[0].reason)
				}
			}
		})
	}
}

func TestRequestAuthorizer_PostLoad_ReRunsLayer1(t *testing.T) {
	// Even if ACL would allow (public-read-write), Layer 1 must still gate.
	audit := &fakeAudit{}
	r := NewRequestAuthorizer(
		stubStore{enabled: true},
		func(_, _, _ string, _ S3Action) bool { return false },
		stubPolicy{allow: true},
		audit,
		func(_ context.Context) string { return "sa-1" },
	)
	in := PermCheckInput{
		Principal: Principal{AccessKey: "AK"},
		Resource:  ResourceRef{Bucket: "b", Key: "k"},
		Action:    GetObject,
		ObjectACL: ACLPublicReadWrite,
	}
	d := r.Decide(context.Background(), in, PhasePostLoad)
	assert.False(t, d.Allow)
	assert.Equal(t, "iam_grant", d.Layer)
	assert.Equal(t, "no_grant", d.Reason)
}
