package admin_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/gritive/GrainFS/internal/iam"
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
		if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s %s: want 404/405, got %d", tc.method, tc.path, resp.StatusCode)
		}
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
		if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s %s: want 404/405, got %d", tc.method, tc.path, resp.StatusCode)
		}
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
	if resp.StatusCode == http.StatusNotFound {
		t.Errorf("GET /ui/api/iam/sa: want route registered (non-404), got 404")
	}
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
		if resp.StatusCode == http.StatusNotFound {
			t.Errorf("%s %s: UDS router must have this route, got 404", tc.method, tc.path)
		}
	}
}
