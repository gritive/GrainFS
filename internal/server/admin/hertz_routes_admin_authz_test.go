package admin_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/iam/principal"
	"github.com/gritive/GrainFS/internal/server/admin"
)

type routeConfigService struct {
	puts    map[string]string
	deletes []string
	entries []config.Entry
}

func (s *routeConfigService) ProposeConfigPut(_ context.Context, key, value string) error {
	if s.puts == nil {
		s.puts = map[string]string{}
	}
	s.puts[key] = value
	return nil
}

func (s *routeConfigService) ProposeConfigDelete(_ context.Context, key string) error {
	s.deletes = append(s.deletes, key)
	return nil
}

func (s *routeConfigService) GetString(key string) (string, bool) {
	for _, entry := range s.entries {
		if entry.Key == key {
			return entry.Value, true
		}
	}
	return "", false
}

func (s *routeConfigService) ListAll() []config.Entry {
	return s.entries
}

func TestConfigAndDashboardRoutesUseBearerActorAuthz(t *testing.T) {
	cfg := &routeConfigService{entries: []config.Entry{{
		Key:     "oidc.enabled",
		Value:   "true",
		Kind:    "bool",
		Default: "false",
		Set:     true,
	}}}
	actor := principal.OIDC("https://idp.example.com/", "alice", "oidc:example:alice", []string{"oidc:example:storage-admins"})
	authz := &credentialAuthorizerStub{decision: policy.DecisionAllow}
	d := newDeps(t)
	d.ConfigProposer = cfg
	d.ConfigStore = cfg
	d.ActorAuth = &actorAuthStub{principal: actor}
	d.AdminAuthz = authz
	cli := startBucketRouteTestServerWithDeps(t, d)

	requireUnixStatus(t, doUnixRouteTestRequestAuth(t, cli, http.MethodGet, "http://unix/v1/config", "Bearer route-token", nil), http.StatusOK)
	requireUnixStatus(t, doUnixRouteTestRequestAuth(t, cli, http.MethodGet, "http://unix/v1/config/oidc.enabled", "Bearer route-token", nil), http.StatusOK)
	requireUnixStatus(t, doUnixRouteTestRequestAuth(t, cli, http.MethodPut, "http://unix/v1/config/oidc.enabled", "Bearer route-token", bytes.NewBufferString(`{"value":"false"}`)), http.StatusNoContent)
	requireUnixStatus(t, doUnixRouteTestRequestAuth(t, cli, http.MethodDelete, "http://unix/v1/config/oidc.enabled", "Bearer route-token", nil), http.StatusNoContent)
	requireUnixStatus(t, doUnixRouteTestRequestAuth(t, cli, http.MethodGet, "http://unix/v1/dashboard/token", "Bearer route-token", nil), http.StatusOK)
	requireUnixStatus(t, doUnixRouteTestRequestAuth(t, cli, http.MethodPost, "http://unix/v1/dashboard/token/rotate", "Bearer route-token", nil), http.StatusOK)

	require.Equal(t, map[string]string{"oidc.enabled": "false"}, cfg.puts)
	require.Equal(t, []string{"oidc.enabled"}, cfg.deletes)
	require.Len(t, authz.calls, 6)
	gotActions := make([]string, 0, len(authz.calls))
	gotResources := make([]string, 0, len(authz.calls))
	for _, call := range authz.calls {
		require.Equal(t, actor, call.principal)
		gotActions = append(gotActions, call.action)
		gotResources = append(gotResources, call.resource)
	}
	require.Equal(t, []string{
		"grainfs:AdminConfigList",
		"grainfs:AdminConfigRead",
		"grainfs:AdminConfigWrite",
		"grainfs:AdminConfigDelete",
		"grainfs:AdminDashboardTokenRead",
		"grainfs:AdminDashboardTokenRotate",
	}, gotActions)
	require.Equal(t, []string{
		"admin/config/*",
		"admin/config/oidc.enabled",
		"admin/config/oidc.enabled",
		"admin/config/oidc.enabled",
		"admin/dashboard/token",
		"admin/dashboard/token/rotate",
	}, gotResources)
}

func TestConfigAndDashboardRoutesDenyBearerBeforeMutation(t *testing.T) {
	cfg := &routeConfigService{entries: []config.Entry{{Key: "oidc.enabled", Value: "true", Set: true}}}
	d := newDeps(t)
	initialToken := d.Token.Get()
	d.ConfigProposer = cfg
	d.ConfigStore = cfg
	d.ActorAuth = &actorAuthStub{principal: principal.OIDC("https://idp.example.com/", "alice", "oidc:example:alice", nil)}
	d.AdminAuthz = &credentialAuthorizerStub{decision: policy.DecisionDeny, reason: "implicit Deny"}
	cli := startBucketRouteTestServerWithDeps(t, d)

	requireUnixStatus(t, doUnixRouteTestRequestAuth(t, cli, http.MethodPut, "http://unix/v1/config/oidc.enabled", "Bearer route-token", bytes.NewBufferString(`{"value":"false"}`)), http.StatusForbidden)
	requireUnixStatus(t, doUnixRouteTestRequestAuth(t, cli, http.MethodPost, "http://unix/v1/dashboard/token/rotate", "Bearer route-token", nil), http.StatusForbidden)

	require.Empty(t, cfg.puts)
	require.Equal(t, initialToken, d.Token.Get())
}

func TestConfigAndDashboardRoutesBearerFailsClosedWithoutAuthorizer(t *testing.T) {
	cfg := &routeConfigService{entries: []config.Entry{{Key: "oidc.enabled", Value: "true", Set: true}}}
	d := newDeps(t)
	d.ConfigProposer = cfg
	d.ConfigStore = cfg
	d.ActorAuth = &actorAuthStub{principal: principal.OIDC("https://idp.example.com/", "alice", "oidc:example:alice", nil)}
	cli := startBucketRouteTestServerWithDeps(t, d)

	requireUnixStatus(t, doUnixRouteTestRequestAuth(t, cli, http.MethodGet, "http://unix/v1/config", "Bearer route-token", nil), http.StatusForbidden)
	requireUnixStatus(t, doUnixRouteTestRequestAuth(t, cli, http.MethodGet, "http://unix/v1/dashboard/token", "Bearer route-token", nil), http.StatusForbidden)
}

func TestConfigAndDashboardRoutesWithoutBearerKeepUDSFallback(t *testing.T) {
	cfg := &routeConfigService{entries: []config.Entry{{Key: "oidc.enabled", Value: "true", Set: true}}}
	authz := &credentialAuthorizerStub{decision: policy.DecisionDeny}
	d := newDeps(t)
	d.ConfigProposer = cfg
	d.ConfigStore = cfg
	d.AdminAuthz = authz
	cli := startBucketRouteTestServerWithDeps(t, d)

	requireUnixStatus(t, doUnixRouteTestRequest(t, cli, http.MethodPut, "http://unix/v1/config/oidc.enabled", bytes.NewBufferString(`{"value":"false"}`)), http.StatusNoContent)
	requireUnixStatus(t, doUnixRouteTestRequest(t, cli, http.MethodPost, "http://unix/v1/dashboard/token/rotate", nil), http.StatusOK)

	require.Equal(t, map[string]string{"oidc.enabled": "false"}, cfg.puts)
	require.Empty(t, authz.calls)
}

func TestDashboardTokenRoutesAbsentFromUIAPI(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	admin.RegisterUI(h, newDeps(t))
	start()

	for _, tc := range []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/ui/api/dashboard/token"},
		{http.MethodPost, "/ui/api/dashboard/token/rotate"},
	} {
		resp := doRouteTestRequest(t, tc.method, base+tc.path, nil)
		resp.Body.Close()
		require.Contains(t, []int{http.StatusNotFound, http.StatusMethodNotAllowed}, resp.StatusCode, "%s %s", tc.method, tc.path)
	}
}

func requireUnixStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	raw, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, want, resp.StatusCode, string(raw))
}
