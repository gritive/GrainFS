package admin_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/iam/principal"
	"github.com/gritive/GrainFS/internal/server/admin"
)

// errStubIAM is returned by stubIAMService methods to signal "not implemented
// for this test stub" — any 5xx from the handler confirms the route is wired.
var errStubIAM = errors.New("stub: not implemented")

// stubIAMService satisfies admin.IAMService for route-registration tests.
// All methods return errStubIAM; no real storage is required.
type stubIAMService struct{}

func (s *stubIAMService) CreateSA(_ context.Context, _ iam.SACreateRequest) (iam.SACreateResponse, error) {
	return iam.SACreateResponse{}, errStubIAM
}
func (s *stubIAMService) ListSA(_ context.Context) ([]iam.SAListItem, error) {
	return nil, errStubIAM
}
func (s *stubIAMService) GetSA(_ context.Context, _ string) (iam.SAGetResponse, error) {
	return iam.SAGetResponse{}, errStubIAM
}
func (s *stubIAMService) DeleteSA(_ context.Context, _ string) error { return errStubIAM }
func (s *stubIAMService) PutGrant(_ context.Context, _ iam.GrantPutRequest) error {
	return errStubIAM
}
func (s *stubIAMService) DeleteGrant(_ context.Context, _ iam.GrantDeleteRequest) error {
	return errStubIAM
}
func (s *stubIAMService) CreateKey(_ context.Context, _ string, _ iam.KeyCreateRequest) (iam.KeyCreateResponse, error) {
	return iam.KeyCreateResponse{}, errStubIAM
}
func (s *stubIAMService) RevokeKey(_ context.Context, _, _ string) error { return errStubIAM }
func (s *stubIAMService) PutBucketUpstream(_ context.Context, _ iam.BucketUpstreamPutRequest) error {
	return errStubIAM
}
func (s *stubIAMService) GetBucketUpstream(_ context.Context, _ string) (iam.BucketUpstreamItem, error) {
	return iam.BucketUpstreamItem{}, errStubIAM
}
func (s *stubIAMService) ListBucketUpstreams(_ context.Context) ([]iam.BucketUpstreamItem, error) {
	return nil, errStubIAM
}
func (s *stubIAMService) DeleteBucketUpstream(_ context.Context, _ string) error { return errStubIAM }
func (s *stubIAMService) CutoverBucketUpstream(_ context.Context, _ string) error {
	return errStubIAM
}

// newRouteTestDeps returns a *admin.Deps with IAM set so that IAM route
// registration is not skipped by the routeFeatureIAM visibility gate.
func newRouteTestDeps() *admin.Deps {
	return &admin.Deps{IAM: &stubIAMService{}}
}

type routeIAMService struct {
	stubIAMService
	listSACalls int
	getSACalls  int
}

func (s *routeIAMService) ListSA(_ context.Context) ([]iam.SAListItem, error) {
	s.listSACalls++
	return []iam.SAListItem{{
		SAID:      "sa-app",
		Name:      "app",
		CreatedAt: time.Unix(100, 0).UTC(),
	}}, nil
}

func (s *routeIAMService) GetSA(_ context.Context, saID string) (iam.SAGetResponse, error) {
	s.getSACalls++
	return iam.SAGetResponse{
		SAID:      saID,
		Name:      "app",
		CreatedAt: time.Unix(100, 0).UTC(),
	}, nil
}

// TestRegisterIAMUI_NoPolicyRoutes asserts that the /ui/api surface does NOT
// expose policy mutation endpoints. A dashboard-token holder must not be able
// to attach Resource:* policies to any SA.
func TestRegisterIAMUI_NoPolicyRoutes(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	admin.RegisterUI(h, newRouteTestDeps())
	start()

	paths := []struct {
		method string
		path   string
	}{
		{http.MethodPut, "/ui/api/iam/policy/my-policy"},
		{http.MethodGet, "/ui/api/iam/policy/my-policy"},
		{http.MethodDelete, "/ui/api/iam/policy/my-policy"},
		{http.MethodGet, "/ui/api/iam/policy"},
		{http.MethodPut, "/ui/api/iam/policy/my-policy/attach/sa/sa-123"},
		{http.MethodDelete, "/ui/api/iam/policy/my-policy/attach/sa/sa-123"},
		{http.MethodPost, "/ui/api/iam/policy/simulate"},
	}
	for _, tc := range paths {
		resp := doRouteTestRequest(t, tc.method, base+tc.path, nil)
		resp.Body.Close()
		require.Contains(t, []int{http.StatusNotFound, http.StatusMethodNotAllowed}, resp.StatusCode, "%s %s", tc.method, tc.path)
	}
}

// TestRegisterIAMUI_NoGroupRoutes asserts that the /ui/api surface does NOT
// expose group management endpoints. A dashboard-token holder must not be able
// to create groups or modify SA membership.
func TestRegisterIAMUI_NoGroupRoutes(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	admin.RegisterUI(h, newRouteTestDeps())
	start()

	paths := []struct {
		method string
		path   string
	}{
		{http.MethodPut, "/ui/api/iam/group/my-group"},
		{http.MethodDelete, "/ui/api/iam/group/my-group"},
		{http.MethodPut, "/ui/api/iam/group/my-group/member/sa-123"},
		{http.MethodDelete, "/ui/api/iam/group/my-group/member/sa-123"},
		{http.MethodPut, "/ui/api/iam/group/my-group/policy/my-policy"},
		{http.MethodDelete, "/ui/api/iam/group/my-group/policy/my-policy"},
	}
	for _, tc := range paths {
		resp := doRouteTestRequest(t, tc.method, base+tc.path, nil)
		resp.Body.Close()
		require.Contains(t, []int{http.StatusNotFound, http.StatusMethodNotAllowed}, resp.StatusCode, "%s %s", tc.method, tc.path)
	}
}

// TestRegisterIAMUI_SARoutePresent is a positive control: the /ui/api surface
// must expose the SA list endpoint so the dashboard remains functional.
func TestRegisterIAMUI_SARoutePresent(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	admin.RegisterUI(h, newRouteTestDeps())
	start()

	resp := doRouteTestRequest(t, http.MethodGet, base+"/ui/api/iam/sa", nil)
	resp.Body.Close()
	// The stub IAM service will cause the handler to return an error status,
	// but the route must exist (not 404). Any non-404 confirms the route is wired.
	require.NotEqual(t, http.StatusNotFound, resp.StatusCode)
}

// TestRegisterIAM_HasPolicyAndGroup confirms that the full UDS surface (via
// RegisterAdmin / RegisterIAMOnly) does expose policy and group routes.
func TestRegisterIAM_HasPolicyAndGroup(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	// RegisterAdmin mounts under /v1 with peerCredMiddleware; use RegisterIAMOnly
	// which mounts IAM routes on /v1 without the peer-cred gate (test-friendly).
	admin.RegisterIAMOnly(h, newRouteTestDeps())
	start()

	paths := []struct {
		method string
		path   string
	}{
		{http.MethodPut, "/v1/iam/policy/my-policy"},
		{http.MethodGet, "/v1/iam/policy/my-policy"},
		{http.MethodDelete, "/v1/iam/policy/my-policy"},
		{http.MethodGet, "/v1/iam/policy"},
		{http.MethodPut, "/v1/iam/group/my-group"},
		{http.MethodDelete, "/v1/iam/group/my-group"},
		{http.MethodPut, "/v1/iam/group/my-group/member/sa-123"},
		{http.MethodDelete, "/v1/iam/group/my-group/member/sa-123"},
		{http.MethodPut, "/v1/iam/group/my-group/policy/my-policy"},
		{http.MethodDelete, "/v1/iam/group/my-group/policy/my-policy"},
	}
	for _, tc := range paths {
		resp := doRouteTestRequest(t, tc.method, base+tc.path, nil)
		resp.Body.Close()
		require.NotEqual(t, http.StatusNotFound, resp.StatusCode, "%s %s", tc.method, tc.path)
	}
}

func TestIAMReadRoutesUseBearerActorAuthz(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	iamSvc := &routeIAMService{}
	policySvc := &fakePolicyService{}
	actor := principal.OIDC("https://idp.example.com/", "alice", "oidc:example:alice", []string{"oidc:example:storage-admins"})
	authz := &credentialAuthorizerStub{decision: policy.DecisionAllow}
	admin.RegisterIAMOnly(h, &admin.Deps{
		IAM:        iamSvc,
		IAMPolicy:  policySvc,
		ActorAuth:  &actorAuthStub{principal: actor},
		AdminAuthz: authz,
	})
	start()

	doRouteTestRequestAuth(t, http.MethodGet, base+"/v1/iam/sa", "Bearer route-token", nil, http.StatusOK)
	doRouteTestRequestAuth(t, http.MethodGet, base+"/v1/iam/sa/sa-app", "Bearer route-token", nil, http.StatusOK)
	doRouteTestRequestAuth(t, http.MethodGet, base+"/v1/iam/policy", "Bearer route-token", nil, http.StatusOK)
	doRouteTestRequestAuth(t, http.MethodGet, base+"/v1/iam/policy/storage-admin", "Bearer route-token", nil, http.StatusNotFound)
	body := bytes.NewBufferString(`{"sa_id":"sa-app","action":"s3:GetObject","resource":"arn:aws:s3:::logs/a"}`)
	doRouteTestRequestAuth(t, http.MethodPost, base+"/v1/iam/policy/simulate", "Bearer route-token", body, http.StatusOK)

	require.Len(t, authz.calls, 5)
	gotActions := []string{
		authz.calls[0].action,
		authz.calls[1].action,
		authz.calls[2].action,
		authz.calls[3].action,
		authz.calls[4].action,
	}
	wantActions := []string{
		"grainfs:IAMServiceAccountList",
		"grainfs:IAMServiceAccountRead",
		"grainfs:IAMPolicyList",
		"grainfs:IAMPolicyRead",
		"grainfs:IAMPolicySimulate",
	}
	require.Equal(t, wantActions, gotActions)
	for i := range authz.calls {
		require.Equal(t, actor, authz.calls[i].principal)
	}
	gotResources := []string{
		authz.calls[0].resource,
		authz.calls[1].resource,
		authz.calls[2].resource,
		authz.calls[3].resource,
		authz.calls[4].resource,
	}
	wantResources := []string{
		"iam/sa/*",
		"iam/sa/sa-app",
		"iam/policy/*",
		"iam/policy/storage-admin",
		"iam/policy/*",
	}
	require.Equal(t, wantResources, gotResources)
}

func TestIAMReadRoutesDenyBearerBeforeHandler(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	iamSvc := &routeIAMService{}
	authz := &credentialAuthorizerStub{decision: policy.DecisionDeny, reason: "implicit Deny"}
	admin.RegisterIAMOnly(h, &admin.Deps{
		IAM:        iamSvc,
		ActorAuth:  &actorAuthStub{principal: principal.OIDC("https://idp.example.com/", "alice", "oidc:example:alice", nil)},
		AdminAuthz: authz,
	})
	start()

	raw := doRouteTestRequestAuth(t, http.MethodGet, base+"/v1/iam/sa", "Bearer route-token", nil, http.StatusForbidden)

	require.Zero(t, iamSvc.listSACalls)
	require.Contains(t, string(raw), "implicit Deny")
}

func TestIAMReadRoutesRejectMalformedBearerBeforeFallback(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	iamSvc := &routeIAMService{}
	authz := &credentialAuthorizerStub{decision: policy.DecisionAllow}
	admin.RegisterIAMOnly(h, &admin.Deps{
		IAM:        iamSvc,
		ActorAuth:  &actorAuthStub{principal: principal.OIDC("https://idp.example.com/", "alice", "oidc:example:alice", nil)},
		AdminAuthz: authz,
	})
	start()

	doRouteTestRequestAuth(t, http.MethodGet, base+"/v1/iam/sa", "Bearer\tbad-token", nil, http.StatusUnauthorized)

	require.Zero(t, iamSvc.listSACalls)
	require.Empty(t, authz.calls)
}

func TestIAMReadRoutesWithoutBearerKeepUDSFallback(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	iamSvc := &routeIAMService{}
	authz := &credentialAuthorizerStub{decision: policy.DecisionDeny}
	admin.RegisterIAMOnly(h, &admin.Deps{IAM: iamSvc, AdminAuthz: authz})
	start()

	doRouteTestRequestAuth(t, http.MethodGet, base+"/v1/iam/sa", "", nil, http.StatusOK)

	require.Equal(t, 1, iamSvc.listSACalls)
	require.Empty(t, authz.calls)
}

func doRouteTestRequestAuth(t *testing.T, method, url, bearer string, body io.Reader, wantStatus int) []byte {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	require.NoError(t, err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		req.Header.Set("Authorization", bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "%s %s", method, url)
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, wantStatus, resp.StatusCode, "%s %s body=%s", method, url, string(raw))
	return raw
}
