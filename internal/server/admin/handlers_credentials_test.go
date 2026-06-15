package admin_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/iam/principal"
	"github.com/gritive/GrainFS/internal/protocred"
	"github.com/gritive/GrainFS/internal/server/admin"
)

func TestCredentialHandlersCreateListGetRotateRevoke(t *testing.T) {
	d := newDeps(t)
	d.ProtocolCredentials = protocred.NewService(protocred.NewStore(), protocred.WithNow(func() time.Time {
		return time.Unix(100, 0).UTC()
	}))
	d.ProtocolCredAuthz = &credentialAuthorizerStub{decision: policy.DecisionAllow}

	created, err := admin.CreateCredential(context.Background(), d, admin.CredentialCreateReq{
		SAID: "node-a", Protocol: "nfs", Resource: "volume/devdisk", Mode: "rw",
	})
	require.NoError(t, err)
	require.NotEmpty(t, created.ID)
	require.NotEmpty(t, created.Secret)
	require.True(t, strings.HasPrefix(created.ConnectionHint["mount_path"], "devdisk/"))

	listed, err := admin.ListCredentials(context.Background(), d, admin.CredentialListReq{SAID: "node-a", Protocol: "nfs"})
	require.NoError(t, err)
	require.Len(t, listed.Credentials, 1)
	require.Empty(t, listed.Credentials[0].Secret)
	require.Equal(t, created.ID, listed.Credentials[0].ID)

	got, err := admin.GetCredential(context.Background(), d, created.ID)
	require.NoError(t, err)
	require.Empty(t, got.Secret)
	require.Equal(t, "node-a", got.SAID)

	rotated, err := admin.RotateCredential(context.Background(), d, created.ID)
	require.NoError(t, err)
	require.Equal(t, created.ID, rotated.ID)
	require.NotEmpty(t, rotated.Secret)
	require.NotEqual(t, created.Secret, rotated.Secret)

	revoked, err := admin.RevokeCredential(context.Background(), d, created.ID)
	require.NoError(t, err)
	require.True(t, revoked.Revoked)
	require.Equal(t, created.ID, revoked.ID)
}

func TestCredentialHandlersUnsupportedWhenServiceMissing(t *testing.T) {
	_, err := admin.CreateCredential(context.Background(), newDeps(t), admin.CredentialCreateReq{
		SAID: "node-a", Protocol: "nfs", Resource: "volume/devdisk", Mode: "rw",
	})
	require.Error(t, err)
	var ae *admin.Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "unsupported", ae.Code)
}

func TestCredentialHandlersRejectInvalidExpiresAt(t *testing.T) {
	d := newDeps(t)
	d.ProtocolCredentials = protocred.NewService(protocred.NewStore())

	_, err := admin.CreateCredential(context.Background(), d, admin.CredentialCreateReq{
		SAID: "node-a", Protocol: "nfs", Resource: "volume/devdisk", Mode: "rw", ExpiresAt: "tomorrow",
	})
	require.Error(t, err)
	var ae *admin.Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "invalid", ae.Code)
}

func TestCredentialHandlersAuthorizeCreateRotateRevoke(t *testing.T) {
	d := newDeps(t)
	d.ProtocolCredentials = protocred.NewService(protocred.NewStore(), protocred.WithNow(func() time.Time {
		return time.Unix(100, 0).UTC()
	}))
	authz := &credentialAuthorizerStub{decision: policy.DecisionAllow}
	d.ProtocolCredAuthz = authz

	created, err := admin.CreateCredential(context.Background(), d, admin.CredentialCreateReq{
		SAID: "sa-app", Protocol: "nfs", Resource: "volume/devdisk", Mode: "rw",
	})
	require.NoError(t, err)
	require.Equal(t, credentialAuthCall{
		principal: principal.ServiceAccount("sa-app"),
		action:    "grainfs:CredentialCreate",
		resource:  "protocol-credential/nfs/volume/devdisk",
	}, authz.calls[0])

	_, err = admin.RotateCredential(context.Background(), d, created.ID)
	require.NoError(t, err)
	require.Equal(t, credentialAuthCall{
		principal: principal.ServiceAccount("sa-app"),
		action:    "grainfs:CredentialRotate",
		resource:  "protocol-credential/nfs/volume/devdisk",
	}, authz.calls[1])

	_, err = admin.RevokeCredential(context.Background(), d, created.ID)
	require.NoError(t, err)
	require.Equal(t, credentialAuthCall{
		principal: principal.ServiceAccount("sa-app"),
		action:    "grainfs:CredentialRevoke",
		resource:  "protocol-credential/nfs/volume/devdisk",
	}, authz.calls[2])
}

func TestCredentialHandlersAuthorizeGetAndList(t *testing.T) {
	d := newDeps(t)
	d.ProtocolCredentials = protocred.NewService(protocred.NewStore(), protocred.WithNow(func() time.Time {
		return time.Unix(100, 0).UTC()
	}))
	authz := &credentialAuthorizerStub{decision: policy.DecisionAllow}
	d.ProtocolCredAuthz = authz

	created, err := admin.CreateCredential(context.Background(), d, admin.CredentialCreateReq{
		SAID: "sa-app", Protocol: "nfs", Resource: "volume/devdisk", Mode: "rw",
	})
	require.NoError(t, err)

	_, err = admin.GetCredential(context.Background(), d, created.ID)
	require.NoError(t, err)
	require.Equal(t, credentialAuthCall{
		principal: principal.ServiceAccount("sa-app"),
		action:    "grainfs:CredentialRead",
		resource:  "protocol-credential/nfs/volume/devdisk",
	}, authz.calls[1])

	_, err = admin.ListCredentials(context.Background(), d, admin.CredentialListReq{Protocol: "nfs", Resource: "volume/devdisk"})
	require.NoError(t, err)
	require.Equal(t, credentialAuthCall{
		principal: principal.ServiceAccount("sa-app"),
		action:    "grainfs:CredentialList",
		resource:  "protocol-credential/nfs/volume/devdisk",
	}, authz.calls[2])
}

func TestCredentialHandlersAuthorizeOIDCActorInsteadOfTargetSA(t *testing.T) {
	d := newDeps(t)
	d.ProtocolCredentials = protocred.NewService(protocred.NewStore())
	authz := &credentialAuthorizerStub{decision: policy.DecisionAllow}
	d.ProtocolCredAuthz = authz
	ctx := admin.WithActorPrincipal(context.Background(), principal.OIDC(
		"https://idp.example.com/",
		"alice",
		"oidc:example:alice",
		[]string{"oidc:example:storage-admins"},
	))

	_, err := admin.CreateCredential(ctx, d, admin.CredentialCreateReq{
		SAID: "sa-app", Protocol: "nfs", Resource: "volume/devdisk", Mode: "rw",
	})

	require.NoError(t, err)
	require.Len(t, authz.calls, 1)
	require.Equal(t, principal.KindOIDC, authz.calls[0].principal.Kind)
	require.Equal(t, "oidc:example:alice", authz.calls[0].principal.ID)
	require.Equal(t, []string{"oidc:example:storage-admins"}, authz.calls[0].principal.Groups)
	require.Equal(t, "grainfs:CredentialCreate", authz.calls[0].action)
	require.Equal(t, "protocol-credential/nfs/volume/devdisk", authz.calls[0].resource)
}

func TestCredentialHandlersDenyCreateBeforeMutation(t *testing.T) {
	d := newDeps(t)
	d.ProtocolCredentials = protocred.NewService(protocred.NewStore())
	d.ProtocolCredAuthz = &credentialAuthorizerStub{decision: policy.DecisionDeny, reason: "implicit Deny"}

	_, err := admin.CreateCredential(context.Background(), d, admin.CredentialCreateReq{
		SAID: "sa-app", Protocol: "nfs", Resource: "volume/devdisk", Mode: "rw",
	})
	requireCredentialForbidden(t, err)

	listed, err := admin.ListCredentials(context.Background(), d, admin.CredentialListReq{})
	require.NoError(t, err)
	require.Empty(t, listed.Credentials)
}

func TestCredentialHandlersValidateCreateBeforeAuthorize(t *testing.T) {
	d := newDeps(t)
	d.ProtocolCredentials = protocred.NewService(protocred.NewStore())
	authz := &credentialAuthorizerStub{decision: policy.DecisionDeny}
	d.ProtocolCredAuthz = authz

	_, err := admin.CreateCredential(context.Background(), d, admin.CredentialCreateReq{
		SAID: "sa-app", Protocol: "bogus", Resource: "volume/devdisk", Mode: "rw",
	})
	require.Error(t, err)
	var ae *admin.Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "invalid", ae.Code)
	require.Empty(t, authz.calls)
}

func TestCredentialHandlersDenyRotateAndRevokeBeforeMutation(t *testing.T) {
	d := newDeps(t)
	d.ProtocolCredentials = protocred.NewService(protocred.NewStore())
	d.ProtocolCredAuthz = &credentialAuthorizerStub{decision: policy.DecisionAllow}

	created, err := admin.CreateCredential(context.Background(), d, admin.CredentialCreateReq{
		SAID: "sa-app", Protocol: "nfs", Resource: "volume/devdisk", Mode: "rw",
	})
	require.NoError(t, err)

	d.ProtocolCredAuthz = &credentialAuthorizerStub{decision: policy.DecisionDeny, reason: "explicit Deny"}
	rotated, err := admin.RotateCredential(context.Background(), d, created.ID)
	requireCredentialForbidden(t, err)
	require.Empty(t, rotated.Secret)

	d.ProtocolCredAuthz = &credentialAuthorizerStub{decision: policy.DecisionAllow}
	got, err := admin.GetCredential(context.Background(), d, created.ID)
	require.NoError(t, err)
	require.Equal(t, created.SecretHint, got.SecretHint)

	d.ProtocolCredAuthz = &credentialAuthorizerStub{decision: policy.DecisionDeny, reason: "explicit Deny"}
	_, err = admin.RevokeCredential(context.Background(), d, created.ID)
	requireCredentialForbidden(t, err)
	d.ProtocolCredAuthz = &credentialAuthorizerStub{decision: policy.DecisionAllow}
	got, err = admin.GetCredential(context.Background(), d, created.ID)
	require.NoError(t, err)
	require.Empty(t, got.RevokedAt)
}

func TestCredentialHandlersDenyGetAndListBeforeRead(t *testing.T) {
	d := newDeps(t)
	d.ProtocolCredentials = protocred.NewService(protocred.NewStore())
	d.ProtocolCredAuthz = &credentialAuthorizerStub{decision: policy.DecisionAllow}

	created, err := admin.CreateCredential(context.Background(), d, admin.CredentialCreateReq{
		SAID: "sa-app", Protocol: "nfs", Resource: "volume/devdisk", Mode: "rw",
	})
	require.NoError(t, err)

	d.ProtocolCredAuthz = &credentialAuthorizerStub{decision: policy.DecisionDeny, reason: "implicit Deny"}
	_, err = admin.GetCredential(context.Background(), d, created.ID)
	requireCredentialForbidden(t, err)

	listed, err := admin.ListCredentials(context.Background(), d, admin.CredentialListReq{Protocol: "nfs", Resource: "volume/devdisk"})
	requireCredentialForbidden(t, err)
	require.Empty(t, listed.Credentials)
}

func TestCredentialHandlersDenyOIDCListEvenWhenEmpty(t *testing.T) {
	d := newDeps(t)
	d.ProtocolCredentials = protocred.NewService(protocred.NewStore())
	authz := &credentialAuthorizerStub{decision: policy.DecisionDeny, reason: "implicit Deny"}
	d.ProtocolCredAuthz = authz
	ctx := admin.WithActorPrincipal(context.Background(), principal.OIDC(
		"https://idp.example.com/",
		"alice",
		"oidc:example:alice",
		[]string{"oidc:example:storage-admins"},
	))

	listed, err := admin.ListCredentials(ctx, d, admin.CredentialListReq{Protocol: "nfs", Resource: "volume/missing"})

	requireCredentialForbidden(t, err)
	require.Empty(t, listed.Credentials)
	require.Len(t, authz.calls, 1)
	require.Equal(t, credentialAuthCall{
		principal: principal.OIDC("https://idp.example.com/", "alice", "oidc:example:alice", []string{"oidc:example:storage-admins"}),
		action:    "grainfs:CredentialList",
		resource:  "protocol-credential/nfs/volume/missing",
	}, authz.calls[0])
}

func TestCredentialHandlersAuthorizeOIDCEmptyListWithPartialFilterResource(t *testing.T) {
	d := newDeps(t)
	d.ProtocolCredentials = protocred.NewService(protocred.NewStore())
	authz := &credentialAuthorizerStub{decision: policy.DecisionAllow}
	d.ProtocolCredAuthz = authz
	ctx := admin.WithActorPrincipal(context.Background(), principal.OIDC(
		"https://idp.example.com/",
		"alice",
		"oidc:example:alice",
		[]string{"oidc:example:storage-admins"},
	))

	listed, err := admin.ListCredentials(ctx, d, admin.CredentialListReq{Protocol: "nfs"})

	require.NoError(t, err)
	require.Empty(t, listed.Credentials)
	require.Len(t, authz.calls, 1)
	require.Equal(t, "grainfs:CredentialList", authz.calls[0].action)
	require.Equal(t, "protocol-credential/nfs/*", authz.calls[0].resource)
}

func TestCredentialHandlersFailClosedWhenAuthorizerMissing(t *testing.T) {
	d := newDeps(t)
	d.ProtocolCredentials = protocred.NewService(protocred.NewStore())

	_, err := admin.CreateCredential(context.Background(), d, admin.CredentialCreateReq{
		SAID: "sa-app", Protocol: "nfs", Resource: "volume/devdisk", Mode: "rw",
	})
	requireCredentialForbidden(t, err)
}

type credentialAuthCall struct {
	principal principal.Principal
	action    string
	resource  string
}

type credentialAuthorizerStub struct {
	decision policy.Decision
	reason   string
	calls    []credentialAuthCall
}

func (s *credentialAuthorizerStub) Authorize(_ context.Context, saID, _ string, ctxReq policy.RequestContext) policy.EvalResult {
	s.calls = append(s.calls, credentialAuthCall{principal: principal.ServiceAccount(saID), action: ctxReq.Action, resource: ctxReq.Resource})
	return policy.EvalResult{Decision: s.decision, Reason: s.reason}
}

func (s *credentialAuthorizerStub) AuthorizePrincipal(_ context.Context, p principal.Principal, _ string, ctxReq policy.RequestContext) policy.EvalResult {
	s.calls = append(s.calls, credentialAuthCall{principal: p, action: ctxReq.Action, resource: ctxReq.Resource})
	return policy.EvalResult{Decision: s.decision, Reason: s.reason}
}

func requireCredentialForbidden(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var ae *admin.Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "forbidden", ae.Code)
	require.Contains(t, ae.Message, "protocol credential permission denied")
}
