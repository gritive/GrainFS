package policy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatchAction_ExactAndWildcard(t *testing.T) {
	tests := []struct {
		pattern, req string
		want         bool
	}{
		{"s3:GetObject", "s3:GetObject", true},
		{"s3:GetObject", "s3:PutObject", false},
		{"s3:*", "s3:GetObject", true},
		{"s3:Get*", "s3:GetObject", true},
		{"s3:Get*", "s3:PutObject", false},
		{"*", "s3:GetObject", true},
		{"s3:*Object", "s3:GetObject", true},
		{"s3:*Object", "s3:ListBucket", false},
	}
	for _, c := range tests {
		require.Equal(t, c.want, matchAction(c.pattern, c.req), "matchAction(%q, %q)", c.pattern, c.req)
	}
}

func TestEvaluateAdminAllowRequiresExplicitAction(t *testing.T) {
	for _, action := range []string{
		"grainfs:BucketPolicyWrite",
		"grainfs:CredentialList",
		"grainfs:IAMPolicyRead",
		"grainfs:AdminConfigWrite",
	} {
		t.Run(action, func(t *testing.T) {
			got := Evaluate(EvalInput{
				PrincipalPolicies: []*Document{{
					Statement: []Statement{{
						Effect:   EffectAllow,
						Action:   StringOrSlice{"*"},
						Resource: StringOrSlice{"*"},
					}},
				}},
				Ctx: RequestContext{Action: action, Resource: "*"},
			})
			require.Equal(t, DecisionDeny, got.Decision, "global wildcard should not allow %s", action)
		})
	}
}

func TestEvaluateAdminAllowAcceptsExplicitAdminWildcard(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		action   string
		resource string
	}{
		{
			name:     "bucket policy",
			pattern:  "grainfs:BucketPolicy*",
			action:   "grainfs:BucketPolicyWrite",
			resource: "arn:aws:s3:::logs",
		},
		{
			name:     "iam policy",
			pattern:  "grainfs:IAMPolicy*",
			action:   "grainfs:IAMPolicyRead",
			resource: "iam/policy/storage-admin",
		},
		{
			name:     "admin config",
			pattern:  "grainfs:Admin*",
			action:   "grainfs:AdminConfigWrite",
			resource: "admin/config/oidc.enabled",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Evaluate(EvalInput{
				PrincipalPolicies: []*Document{{
					Statement: []Statement{{
						Effect:   EffectAllow,
						Action:   StringOrSlice{tc.pattern},
						Resource: StringOrSlice{tc.resource},
					}},
				}},
				Ctx: RequestContext{Action: tc.action, Resource: tc.resource},
			})
			require.Equal(t, DecisionAllow, got.Decision)
		})
	}
}

func TestEvaluateAdminDenyStillAcceptsGlobalWildcard(t *testing.T) {
	got := Evaluate(EvalInput{
		PrincipalPolicies: []*Document{{
			Statement: []Statement{{
				Effect:   EffectDeny,
				Action:   StringOrSlice{"*"},
				Resource: StringOrSlice{"*"},
			}, {
				Effect:   EffectAllow,
				Action:   StringOrSlice{"grainfs:BucketPolicyWrite"},
				Resource: StringOrSlice{"arn:aws:s3:::logs"},
			}},
		}},
		Ctx: RequestContext{Action: "grainfs:BucketPolicyWrite", Resource: "arn:aws:s3:::logs"},
	})
	require.Equal(t, DecisionDeny, got.Decision, "global wildcard deny should still deny admin actions")
}

func TestEvaluateAdminAllowRejectsGrainFSWildcard(t *testing.T) {
	got := Evaluate(EvalInput{
		PrincipalPolicies: []*Document{{
			Statement: []Statement{{
				Effect:   EffectAllow,
				Action:   StringOrSlice{"grainfs:*"},
				Resource: StringOrSlice{"admin/config/oidc.enabled"},
			}},
		}},
		Ctx: RequestContext{Action: "grainfs:AdminConfigWrite", Resource: "admin/config/oidc.enabled"},
	})
	require.Equal(t, DecisionDeny, got.Decision)
}

func TestMatchResource_PrefixBoundary(t *testing.T) {
	// F#10: arn:aws:s3:::analytics/logs/* must NOT match analytics/logsx/secret
	require.False(t, matchResource("arn:aws:s3:::analytics/logs/*", "arn:aws:s3:::analytics/logsx/secret"))
	require.True(t, matchResource("arn:aws:s3:::analytics/logs/*", "arn:aws:s3:::analytics/logs/2026-05.json"))
}

func TestMatchResource_IAMAdminWildcardIsSegmentScoped(t *testing.T) {
	require.True(t, matchResource("iam/policy/*", "iam/policy/storage-admin"))
	require.False(t, matchResource("iam/policy/*", "iam/policy/storage-admin/attach/sa/sa-app"))
	require.True(t, matchResource("iam/policy/*/attach/sa/*", "iam/policy/storage-admin/attach/sa/sa-app"))
	require.False(t, matchResource("iam/group/*", "iam/group/admins/policy/storage-admin"))
	require.True(t, matchResource("iam/group/*/policy/*", "iam/group/admins/policy/storage-admin"))
}

func TestMatchResource_GrainFSAdminWildcardIsSegmentScoped(t *testing.T) {
	require.True(t, matchResource("admin/config/*", "admin/config/oidc.enabled"))
	require.False(t, matchResource("admin/config/*", "admin/config/oidc/enabled"))
	require.True(t, matchResource("admin/dashboard/token", "admin/dashboard/token"))
	require.False(t, matchResource("admin/dashboard/token", "admin/dashboard/token/rotate"))
	require.True(t, matchResource("admin/dashboard/token/*", "admin/dashboard/token/rotate"))
}

func TestEvaluateIAMMutationRequiresNestedResourceGrant(t *testing.T) {
	got := Evaluate(EvalInput{
		PrincipalPolicies: []*Document{{
			Statement: []Statement{{
				Effect:   EffectAllow,
				Action:   StringOrSlice{"grainfs:IAMPolicyAttach"},
				Resource: StringOrSlice{"iam/policy/*"},
			}},
		}},
		Ctx: RequestContext{
			Action:   "grainfs:IAMPolicyAttach",
			Resource: "iam/policy/storage-admin/attach/sa/sa-app",
		},
	})
	require.Equal(t, DecisionDeny, got.Decision)

	got = Evaluate(EvalInput{
		PrincipalPolicies: []*Document{{
			Statement: []Statement{{
				Effect:   EffectAllow,
				Action:   StringOrSlice{"grainfs:IAMPolicyAttach"},
				Resource: StringOrSlice{"iam/policy/*/attach/sa/*"},
			}},
		}},
		Ctx: RequestContext{
			Action:   "grainfs:IAMPolicyAttach",
			Resource: "iam/policy/storage-admin/attach/sa/sa-app",
		},
	})
	require.Equal(t, DecisionAllow, got.Decision)
}

func TestMatchCondition_SourceIpAllow(t *testing.T) {
	cond := map[string]map[string]StringOrSlice{
		"IpAddress": {"aws:SourceIp": []string{"10.0.0.0/8"}},
	}
	require.True(t, matchCondition(cond, RequestContext{SourceIP: "10.1.2.3"}))
	require.False(t, matchCondition(cond, RequestContext{SourceIP: "192.168.0.1"}))
}

func TestMatchCondition_S3PrefixOnListOnly(t *testing.T) {
	cond := map[string]map[string]StringOrSlice{
		"StringLike": {"s3:prefix": []string{"logs/*"}},
	}
	require.False(t, matchCondition(cond, RequestContext{Action: "s3:GetObject"}))
	require.True(t, matchCondition(cond, RequestContext{Action: "s3:ListBucket", Prefix: "logs/2026"}))
}
