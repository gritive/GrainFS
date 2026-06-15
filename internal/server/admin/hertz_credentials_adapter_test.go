package admin_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/iam/principal"
	"github.com/gritive/GrainFS/internal/protocred"
	"github.com/gritive/GrainFS/internal/server/admin"
)

func TestCredentialRoutesUseBearerActorForAllActions(t *testing.T) {
	d := newCredentialRouteDeps(t)
	actor := principal.OIDC("https://idp.example.com/", "alice", "oidc:example:alice", []string{"oidc:example:storage-admins"})
	d.ActorAuth = &actorAuthStub{principal: actor}
	authz := &credentialAuthorizerStub{decision: policy.DecisionAllow}
	d.ProtocolCredAuthz = authz
	cli := startCredentialRouteTestServer(t, d)

	created := doCredentialCreateHTTP(t, cli, "bearer route-token")
	require.Equal(t, "pc_", created.ID[:3])

	listResp := doCredentialListHTTP(t, cli, "Bearer route-token", http.StatusOK)
	require.Len(t, listResp.Credentials, 1)
	doCredentialHTTP(t, cli, http.MethodGet, "http://unix/v1/credentials/"+created.ID, "Bearer route-token", nil, http.StatusOK)
	doCredentialHTTP(t, cli, http.MethodPost, "http://unix/v1/credentials/"+created.ID+"/rotate", "Bearer route-token", nil, http.StatusOK)
	doCredentialHTTP(t, cli, http.MethodDelete, "http://unix/v1/credentials/"+created.ID, "Bearer route-token", nil, http.StatusOK)

	require.Len(t, authz.calls, 5)
	for _, call := range authz.calls {
		require.Equal(t, principal.KindOIDC, call.principal.Kind)
		require.Equal(t, "oidc:example:alice", call.principal.ID)
	}
	require.Equal(t, []string{
		"grainfs:CredentialCreate",
		"grainfs:CredentialList",
		"grainfs:CredentialRead",
		"grainfs:CredentialRotate",
		"grainfs:CredentialRevoke",
	}, []string{
		authz.calls[0].action,
		authz.calls[1].action,
		authz.calls[2].action,
		authz.calls[3].action,
		authz.calls[4].action,
	})
}

func TestCredentialRoutesWithoutBearerKeepServiceAccountFallback(t *testing.T) {
	d := newCredentialRouteDeps(t)
	authz := &credentialAuthorizerStub{decision: policy.DecisionAllow}
	d.ProtocolCredAuthz = authz
	cli := startCredentialRouteTestServer(t, d)

	created := doCredentialCreateHTTP(t, cli, "")

	require.NotEmpty(t, created.ID)
	require.Len(t, authz.calls, 1)
	require.Equal(t, principal.ServiceAccount("sa-app"), authz.calls[0].principal)
}

func TestCredentialRoutesRejectInvalidBearerBeforeHandler(t *testing.T) {
	d := newCredentialRouteDeps(t)
	d.ActorAuth = &actorAuthStub{err: errors.New("bad token")}
	authz := &credentialAuthorizerStub{decision: policy.DecisionAllow}
	d.ProtocolCredAuthz = authz
	cli := startCredentialRouteTestServer(t, d)

	doCredentialHTTP(t, cli, http.MethodPost, "http://unix/v1/credentials", "Bearer bad-token", credentialCreateBody(), http.StatusUnauthorized)

	require.Empty(t, authz.calls)
	listed := d.ProtocolCredentials.List(protocred.ListFilter{})
	require.Empty(t, listed)
}

func TestCredentialRoutesRejectEmptyBearer(t *testing.T) {
	d := newCredentialRouteDeps(t)
	d.ActorAuth = &actorAuthStub{principal: principal.OIDC("https://idp.example.com/", "alice", "oidc:example:alice", nil)}
	d.ProtocolCredAuthz = &credentialAuthorizerStub{decision: policy.DecisionAllow}
	cli := startCredentialRouteTestServer(t, d)

	doCredentialHTTP(t, cli, http.MethodPost, "http://unix/v1/credentials", "Bearer ", credentialCreateBody(), http.StatusUnauthorized)
}

func TestCredentialRoutesRejectMalformedBearerBeforeFallback(t *testing.T) {
	for _, bearer := range []string{"Bearer\tbad-token", "BearerX bad-token"} {
		t.Run(bearer, func(t *testing.T) {
			d := newCredentialRouteDeps(t)
			authz := &credentialAuthorizerStub{decision: policy.DecisionAllow}
			d.ProtocolCredAuthz = authz
			cli := startCredentialRouteTestServer(t, d)

			doCredentialHTTP(t, cli, http.MethodPost, "http://unix/v1/credentials", bearer, credentialCreateBody(), http.StatusUnauthorized)

			require.Empty(t, authz.calls)
			require.Empty(t, d.ProtocolCredentials.List(protocred.ListFilter{}))
		})
	}
}

func TestCredentialRoutesDenyBeforeCreateAndRotateMutation(t *testing.T) {
	d := newCredentialRouteDeps(t)
	d.ActorAuth = &actorAuthStub{principal: principal.OIDC("https://idp.example.com/", "alice", "oidc:example:alice", nil)}
	authz := &credentialAuthorizerStub{decision: policy.DecisionDeny, reason: "implicit Deny"}
	d.ProtocolCredAuthz = authz
	cli := startCredentialRouteTestServer(t, d)

	doCredentialHTTP(t, cli, http.MethodPost, "http://unix/v1/credentials", "Bearer route-token", credentialCreateBody(), http.StatusForbidden)
	require.Empty(t, d.ProtocolCredentials.List(protocred.ListFilter{}))

	authz.decision = policy.DecisionAllow
	created := doCredentialCreateHTTP(t, cli, "Bearer route-token")
	authz.decision = policy.DecisionDeny
	rotate := doCredentialHTTP(t, cli, http.MethodPost, "http://unix/v1/credentials/"+created.ID+"/rotate", "Bearer route-token", nil, http.StatusForbidden)
	require.NotContains(t, string(rotate), "secret")

	authz.decision = policy.DecisionAllow
	got := doCredentialHTTP(t, cli, http.MethodGet, "http://unix/v1/credentials/"+created.ID, "Bearer route-token", nil, http.StatusOK)
	require.Contains(t, string(got), created.SecretHint)
}

func TestCredentialBearerMiddlewareDoesNotAffectOtherAdminRoutes(t *testing.T) {
	d := newCredentialRouteDeps(t)
	d.ActorAuth = &actorAuthStub{err: errors.New("bad token")}
	d.ProtocolCredAuthz = &credentialAuthorizerStub{decision: policy.DecisionAllow}
	cli := startCredentialRouteTestServer(t, d)

	raw := doCredentialHTTP(t, cli, http.MethodGet, "http://unix/v1/cluster/peers", "Bearer bad-token", nil, http.StatusOK)

	require.Contains(t, string(raw), `"peers"`)
}

type actorAuthStub struct {
	principal principal.Principal
	err       error
	tokens    []string
}

func (s *actorAuthStub) AuthenticateActor(_ context.Context, bearerToken string) (principal.Principal, error) {
	s.tokens = append(s.tokens, bearerToken)
	if s.err != nil {
		return principal.Principal{}, s.err
	}
	return s.principal, nil
}

func newCredentialRouteDeps(t *testing.T) *admin.Deps {
	t.Helper()
	d := newDeps(t)
	d.ProtocolCredentials = protocred.NewService(protocred.NewStore(), protocred.WithNow(func() time.Time {
		return time.Unix(100, 0).UTC()
	}))
	return d
}

func startCredentialRouteTestServer(t *testing.T, d *admin.Deps) *http.Client {
	t.Helper()
	dir, err := os.MkdirTemp("", "grainfs-admin-credential-route-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	sock := filepath.Join(dir, "admin.sock")
	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)
	h := server.New(
		server.WithListener(ln),
		server.WithTransport(standard.NewTransporter),
		server.WithHostPorts(""),
		server.WithExitWaitTime(10*time.Millisecond),
	)
	admin.RegisterAdmin(h, d)
	go h.Spin() //nolint:errcheck
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_ = h.Shutdown(ctx)
	})
	waitForUnixSocket(t, sock)
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}}
}

func doCredentialCreateHTTP(t *testing.T, cli *http.Client, bearer string) admin.CredentialResp {
	t.Helper()
	raw := doCredentialHTTP(t, cli, http.MethodPost, "http://unix/v1/credentials", bearer, credentialCreateBody(), http.StatusCreated)
	var resp admin.CredentialResp
	require.NoError(t, json.Unmarshal(raw, &resp))
	return resp
}

func doCredentialListHTTP(t *testing.T, cli *http.Client, bearer string, wantStatus int) admin.CredentialListResp {
	t.Helper()
	raw := doCredentialHTTP(t, cli, http.MethodGet, "http://unix/v1/credentials?protocol=nfs&resource=volume/devdisk", bearer, nil, wantStatus)
	var resp admin.CredentialListResp
	require.NoError(t, json.Unmarshal(raw, &resp))
	return resp
}

func doCredentialHTTP(t *testing.T, cli *http.Client, method, url, bearer string, body io.Reader, wantStatus int) []byte {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	require.NoError(t, err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		req.Header.Set("Authorization", bearer)
	}
	resp, err := cli.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, wantStatus, resp.StatusCode, string(raw))
	return raw
}

func credentialCreateBody() io.Reader {
	return bytes.NewBufferString(`{"sa_id":"sa-app","protocol":"nfs","resource":"volume/devdisk","mode":"rw"}`)
}
