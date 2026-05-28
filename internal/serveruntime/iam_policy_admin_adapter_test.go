package serveruntime

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/iam/principal"
	"github.com/gritive/GrainFS/internal/server/admin"
)

func TestIAMPolicyAdminAdapter_SimulateOIDCGroupPolicy(t *testing.T) {
	ctx := context.Background()
	stores, err := WireIAMPolicyStores(ctx, nil, time.Hour)
	require.NoError(t, err)
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)
	require.NoError(t, stores.Policies.Put(ctx, "oidc-read", doc, false))
	require.NoError(t, stores.Attach.AttachToGroup(ctx, "oidc:example:data-eng", "oidc-read"))
	adapter := NewIAMPolicyAdminAdapter(stores, func(context.Context, clusterpb.MetaCmdType, []byte) error { return nil })
	issuer := "https://idp.example.com/"
	subject := "user@example.com"
	res, err := adapter.Simulate(ctx, admin.PolicySimulateRequest{
		PrincipalKind: "oidc",
		PrincipalID:   testOIDCPrincipalID(issuer, subject),
		Issuer:        issuer,
		Subject:       subject,
		Groups:        []string{"oidc:example:data-eng"},
		Action:        "s3:GetObject",
		Resource:      "arn:aws:s3:::bucket/key",
	})
	require.NoError(t, err)
	require.Equal(t, "Allow", res.Effect)
	require.Equal(t, "oidc-read", res.MatchedPolicy)
	require.Equal(t, "oidc", res.PrincipalKind)
	require.NotEmpty(t, res.PrincipalID)
	require.Len(t, res.Groups, 1)
}

func TestIAMPolicyAdminAdapter_RejectsOIDCPrincipalIDMismatch(t *testing.T) {
	ctx := context.Background()
	stores, err := WireIAMPolicyStores(ctx, nil, time.Hour)
	require.NoError(t, err)
	adapter := NewIAMPolicyAdminAdapter(stores, func(context.Context, clusterpb.MetaCmdType, []byte) error { return nil })
	_, err = adapter.Simulate(ctx, admin.PolicySimulateRequest{
		PrincipalKind: "oidc",
		PrincipalID:   "oidc:wrong",
		Issuer:        "https://idp.example.com/",
		Subject:       "user@example.com",
		Action:        "s3:GetObject",
		Resource:      "arn:aws:s3:::bucket/key",
	})
	require.Error(t, err)
	var ae *admin.Error
	require.True(t, errors.As(err, &ae), "expected admin error, got %T: %v", err, err)
	require.Equal(t, "invalid", ae.Code)
}

func TestIAMPolicyAdminAdapter_SelfEffectDetectsOIDCPolicyAndGroup(t *testing.T) {
	ctx := context.Background()
	stores, err := WireIAMPolicyStores(ctx, nil, time.Hour)
	require.NoError(t, err)
	require.NoError(t, stores.Attach.AttachToGroup(ctx, "oidc:example:admins", "iam-admin"))

	guard := NewIAMPolicyAdminAdapter(stores, func(context.Context, clusterpb.MetaCmdType, []byte) error { return nil }).(admin.AdminSelfEffectGuard)
	actor := principal.OIDC("https://idp.example.com/", "alice", "oidc:example:alice", []string{"oidc:example:admins"})

	affects, err := guard.PolicyAffectsPrincipal(ctx, actor, "iam-admin")
	require.NoError(t, err)
	require.True(t, affects)
	affects, err = guard.GroupAffectsPrincipal(ctx, actor, "oidc:example:admins")
	require.NoError(t, err)
	require.True(t, affects)
	affects, err = guard.PolicyAffectsPrincipal(ctx, actor, "unrelated")
	require.NoError(t, err)
	require.False(t, affects)
}

func TestIAMPolicyAdminAdapter_SelfEffectDetectsServiceAccountGroupPolicy(t *testing.T) {
	ctx := context.Background()
	stores, err := WireIAMPolicyStores(ctx, nil, time.Hour)
	require.NoError(t, err)
	require.NoError(t, stores.Groups.Put(ctx, "admins", nil))
	require.NoError(t, stores.Groups.AddMember(ctx, "admins", "sa-1"))
	require.NoError(t, stores.Attach.AttachToGroup(ctx, "admins", "iam-admin"))

	guard := NewIAMPolicyAdminAdapter(stores, func(context.Context, clusterpb.MetaCmdType, []byte) error { return nil }).(admin.AdminSelfEffectGuard)
	actor := principal.ServiceAccount("sa-1")

	affects, err := guard.PolicyAffectsPrincipal(ctx, actor, "iam-admin")
	require.NoError(t, err)
	require.True(t, affects)
	affects, err = guard.GroupAffectsPrincipal(ctx, actor, "admins")
	require.NoError(t, err)
	require.True(t, affects)
}

func testOIDCPrincipalID(issuer, subject string) string {
	issuerSum := sha256.Sum256([]byte(issuer))
	subjectSum := sha256.Sum256([]byte(subject))
	return fmt.Sprintf("oidc:%x:%x", issuerSum[:8], subjectSum[:16])
}
