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

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
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
	listSACalls      int
	getSACalls       int
	upstreamPuts     []iam.BucketUpstreamPutRequest
	upstreamGets     []string
	upstreamLists    int
	upstreamDeletes  []string
	upstreamCutovers []string
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

func (s *routeIAMService) PutBucketUpstream(_ context.Context, req iam.BucketUpstreamPutRequest) error {
	s.upstreamPuts = append(s.upstreamPuts, req)
	return nil
}

func (s *routeIAMService) GetBucketUpstream(_ context.Context, bucket string) (iam.BucketUpstreamItem, error) {
	s.upstreamGets = append(s.upstreamGets, bucket)
	return iam.BucketUpstreamItem{
		Bucket:      bucket,
		UpstreamURL: "https://s3.example.com",
		AccessKey:   "AKIA",
		CreatedAt:   time.Unix(100, 0).UTC(),
		Status:      iam.BucketUpstreamStatusActive,
	}, nil
}

func (s *routeIAMService) ListBucketUpstreams(_ context.Context) ([]iam.BucketUpstreamItem, error) {
	s.upstreamLists++
	return []iam.BucketUpstreamItem{{
		Bucket:      "logs",
		UpstreamURL: "https://s3.example.com",
		AccessKey:   "AKIA",
		CreatedAt:   time.Unix(100, 0).UTC(),
		Status:      iam.BucketUpstreamStatusActive,
	}}, nil
}

func (s *routeIAMService) DeleteBucketUpstream(_ context.Context, bucket string) error {
	s.upstreamDeletes = append(s.upstreamDeletes, bucket)
	return nil
}

func (s *routeIAMService) CutoverBucketUpstream(_ context.Context, bucket string) error {
	s.upstreamCutovers = append(s.upstreamCutovers, bucket)
	return nil
}

type routeIAMGroupService struct {
	proposed []clusterpb.MetaCmdType
}

func (s *routeIAMGroupService) Propose(_ context.Context, cmdType clusterpb.MetaCmdType, _ []byte) error {
	s.proposed = append(s.proposed, cmdType)
	return nil
}

type routeIAMMountSAService struct {
	proposed []clusterpb.MetaCmdType
}

func (s *routeIAMMountSAService) Propose(_ context.Context, cmdType clusterpb.MetaCmdType, _ []byte) error {
	s.proposed = append(s.proposed, cmdType)
	return nil
}

func (s *routeIAMMountSAService) List() []admin.MountSAItem {
	return []admin.MountSAItem{{Name: "mount", NumericUID: 1000, CreatedAt: 100}}
}

func (s *routeIAMMountSAService) Get(name string) (admin.MountSAItem, bool) {
	return admin.MountSAItem{Name: name, NumericUID: 1000, CreatedAt: 100}, true
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

func TestIAMMutationAndGroupRoutesUseBearerActorAuthz(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	actor := principal.OIDC("https://idp.example.com/", "alice", "oidc:example:alice", []string{"oidc:example:storage-admins"})
	authz := &credentialAuthorizerStub{decision: policy.DecisionAllow}
	admin.RegisterIAMOnly(h, &admin.Deps{
		IAM:        &routeIAMService{},
		IAMPolicy:  &fakePolicyService{},
		IAMGroup:   &routeIAMGroupService{},
		ActorAuth:  &actorAuthStub{principal: actor},
		AdminAuthz: authz,
	})
	start()

	body := bytes.NewBufferString(`{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)
	doRouteTestRequestAuth(t, http.MethodPut, base+"/v1/iam/policy/storage-admin", "Bearer route-token", body, http.StatusNoContent)
	doRouteTestRequestAuth(t, http.MethodDelete, base+"/v1/iam/policy/storage-admin", "Bearer route-token", nil, http.StatusNoContent)
	doRouteTestRequestAuth(t, http.MethodPut, base+"/v1/iam/policy/storage-admin/attach/sa/sa-app", "Bearer route-token", nil, http.StatusNoContent)
	doRouteTestRequestAuth(t, http.MethodDelete, base+"/v1/iam/policy/storage-admin/attach/sa/sa-app", "Bearer route-token", nil, http.StatusNoContent)
	doRouteTestRequestAuth(t, http.MethodPut, base+"/v1/iam/group/storage-admins", "Bearer route-token", nil, http.StatusNoContent)
	doRouteTestRequestAuth(t, http.MethodDelete, base+"/v1/iam/group/storage-admins", "Bearer route-token", nil, http.StatusNoContent)
	doRouteTestRequestAuth(t, http.MethodPut, base+"/v1/iam/group/storage-admins/member/sa-app", "Bearer route-token", nil, http.StatusNoContent)
	doRouteTestRequestAuth(t, http.MethodDelete, base+"/v1/iam/group/storage-admins/member/sa-app", "Bearer route-token", nil, http.StatusNoContent)
	doRouteTestRequestAuth(t, http.MethodPut, base+"/v1/iam/group/storage-admins/policy/storage-admin", "Bearer route-token", nil, http.StatusNoContent)
	doRouteTestRequestAuth(t, http.MethodDelete, base+"/v1/iam/group/storage-admins/policy/storage-admin", "Bearer route-token", nil, http.StatusNoContent)

	require.Len(t, authz.calls, 10)
	gotActions := make([]string, 0, len(authz.calls))
	gotResources := make([]string, 0, len(authz.calls))
	for _, call := range authz.calls {
		require.Equal(t, actor, call.principal)
		gotActions = append(gotActions, call.action)
		gotResources = append(gotResources, call.resource)
	}
	require.Equal(t, []string{
		"grainfs:IAMPolicyWrite",
		"grainfs:IAMPolicyDelete",
		"grainfs:IAMPolicyAttach",
		"grainfs:IAMPolicyDetach",
		"grainfs:IAMGroupWrite",
		"grainfs:IAMGroupDelete",
		"grainfs:IAMGroupMemberWrite",
		"grainfs:IAMGroupMemberDelete",
		"grainfs:IAMGroupPolicyAttach",
		"grainfs:IAMGroupPolicyDetach",
	}, gotActions)
	require.Equal(t, []string{
		"iam/policy/storage-admin",
		"iam/policy/storage-admin",
		"iam/policy/storage-admin/attach/sa/sa-app",
		"iam/policy/storage-admin/attach/sa/sa-app",
		"iam/group/storage-admins",
		"iam/group/storage-admins",
		"iam/group/storage-admins",
		"iam/group/storage-admins",
		"iam/group/storage-admins/policy/storage-admin",
		"iam/group/storage-admins/policy/storage-admin",
	}, gotResources)
}

func TestIAMMountSAAndUpstreamRoutesUseBearerActorAuthz(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	actor := principal.OIDC("https://idp.example.com/", "alice", "oidc:example:alice", []string{"oidc:example:storage-admins"})
	authz := &credentialAuthorizerStub{decision: policy.DecisionAllow}
	iamSvc := &routeIAMService{}
	mountSvc := &routeIAMMountSAService{}
	admin.RegisterIAMOnly(h, &admin.Deps{
		IAM:        iamSvc,
		IAMPolicy:  &fakePolicyService{},
		IAMMountSA: mountSvc,
		ActorAuth:  &actorAuthStub{principal: actor},
		AdminAuthz: authz,
	})
	start()

	upstreamBody := bytes.NewBufferString(`{"bucket":"logs","upstream_url":"https://s3.example.com","access_key":"AKIA","secret_key":"secret"}`)
	cutoverBody := bytes.NewBufferString(`{"bucket":"logs"}`)
	createMountBody := bytes.NewBufferString(`{"name":"mount","uid":1000}`)
	doRouteTestRequestAuth(t, http.MethodPut, base+"/v1/iam/mount-sa/mount/policy/NFSMountOnly", "Bearer route-token", nil, http.StatusNoContent)
	doRouteTestRequestAuth(t, http.MethodDelete, base+"/v1/iam/mount-sa/mount/policy/NFSMountOnly", "Bearer route-token", nil, http.StatusNoContent)
	doRouteTestRequestAuth(t, http.MethodPost, base+"/v1/iam/mount-sa", "Bearer route-token", createMountBody, http.StatusCreated)
	doRouteTestRequestAuth(t, http.MethodGet, base+"/v1/iam/mount-sa", "Bearer route-token", nil, http.StatusOK)
	doRouteTestRequestAuth(t, http.MethodGet, base+"/v1/iam/mount-sa/mount", "Bearer route-token", nil, http.StatusOK)
	doRouteTestRequestAuth(t, http.MethodDelete, base+"/v1/iam/mount-sa/mount", "Bearer route-token", nil, http.StatusNoContent)
	doRouteTestRequestAuth(t, http.MethodPut, base+"/v1/upstreams", "Bearer route-token", upstreamBody, http.StatusNoContent)
	doRouteTestRequestAuth(t, http.MethodGet, base+"/v1/upstreams", "Bearer route-token", nil, http.StatusOK)
	doRouteTestRequestAuth(t, http.MethodGet, base+"/v1/buckets/logs/upstream", "Bearer route-token", nil, http.StatusOK)
	doRouteTestRequestAuth(t, http.MethodDelete, base+"/v1/buckets/logs/upstream", "Bearer route-token", nil, http.StatusNoContent)
	doRouteTestRequestAuth(t, http.MethodPost, base+"/v1/migration/cutover", "Bearer route-token", cutoverBody, http.StatusNoContent)

	require.Len(t, authz.calls, 11)
	gotActions := make([]string, 0, len(authz.calls))
	gotResources := make([]string, 0, len(authz.calls))
	for _, call := range authz.calls {
		require.Equal(t, actor, call.principal)
		gotActions = append(gotActions, call.action)
		gotResources = append(gotResources, call.resource)
	}
	require.Equal(t, []string{
		"grainfs:IAMMountSAPolicyAttach",
		"grainfs:IAMMountSAPolicyDetach",
		"grainfs:IAMMountSAWrite",
		"grainfs:IAMMountSAList",
		"grainfs:IAMMountSARead",
		"grainfs:IAMMountSADelete",
		"grainfs:IAMBucketUpstreamWrite",
		"grainfs:IAMBucketUpstreamList",
		"grainfs:IAMBucketUpstreamRead",
		"grainfs:IAMBucketUpstreamDelete",
		"grainfs:IAMBucketUpstreamCutover",
	}, gotActions)
	require.Equal(t, []string{
		"iam/mount-sa/mount/policy/NFSMountOnly",
		"iam/mount-sa/mount/policy/NFSMountOnly",
		"iam/mount-sa/mount",
		"iam/mount-sa/*",
		"iam/mount-sa/mount",
		"iam/mount-sa/mount",
		"iam/upstream/logs",
		"iam/upstream/*",
		"iam/upstream/logs",
		"iam/upstream/logs",
		"iam/upstream/logs/cutover",
	}, gotResources)
	require.Equal(t, []clusterpb.MetaCmdType{
		clusterpb.MetaCmdTypeMountSAAttachPolicy,
		clusterpb.MetaCmdTypeMountSADetachPolicy,
		clusterpb.MetaCmdTypeMountSACreate,
		clusterpb.MetaCmdTypeMountSADelete,
	}, mountSvc.proposed)
	require.Equal(t, []iam.BucketUpstreamPutRequest{{
		Bucket:      "logs",
		UpstreamURL: "https://s3.example.com",
		AccessKey:   "AKIA",
		SecretKey:   "secret",
	}}, iamSvc.upstreamPuts)
	require.Equal(t, 1, iamSvc.upstreamLists)
	require.Equal(t, []string{"logs"}, iamSvc.upstreamGets)
	require.Equal(t, []string{"logs"}, iamSvc.upstreamDeletes)
	require.Equal(t, []string{"logs"}, iamSvc.upstreamCutovers)
}

func TestIAMMutationRoutesDenyBearerSelfEffectBeforeHandler(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	policySvc := &fakePolicyService{
		selfPolicies: map[string]bool{"self-admin": true},
		selfGroups:   map[string]bool{"oidc:example:admins": true},
	}
	groupSvc := &routeIAMGroupService{}
	authz := &credentialAuthorizerStub{decision: policy.DecisionAllow}
	actor := principal.ServiceAccount("sa-self")
	admin.RegisterIAMOnly(h, &admin.Deps{
		IAM:        &routeIAMService{},
		IAMPolicy:  policySvc,
		IAMGroup:   groupSvc,
		ActorAuth:  &actorAuthStub{principal: actor},
		AdminAuthz: authz,
	})
	start()

	body := bytes.NewBufferString(`{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)
	raw := doRouteTestRequestAuth(t, http.MethodPut, base+"/v1/iam/policy/self-admin", "Bearer route-token", body, http.StatusForbidden)
	require.Contains(t, string(raw), "policy affects caller")
	raw = doRouteTestRequestAuth(t, http.MethodPut, base+"/v1/iam/policy/self-admin/attach/sa/sa-self", "Bearer route-token", nil, http.StatusForbidden)
	require.Contains(t, string(raw), "direct policy attachment")
	raw = doRouteTestRequestAuth(t, http.MethodPut, base+"/v1/iam/group/oidc:example:admins", "Bearer route-token", nil, http.StatusForbidden)
	require.Contains(t, string(raw), "group affects caller")
	raw = doRouteTestRequestAuth(t, http.MethodPut, base+"/v1/iam/group/storage-admins/member/sa-self", "Bearer route-token", nil, http.StatusForbidden)
	require.Contains(t, string(raw), "group membership")
	raw = doRouteTestRequestAuth(t, http.MethodPut, base+"/v1/iam/group/oidc:example:admins/policy/self-admin", "Bearer route-token", nil, http.StatusForbidden)
	require.Contains(t, string(raw), "group affects caller")

	require.Empty(t, policySvc.proposed)
	require.Empty(t, groupSvc.proposed)
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

func TestIAMMountSAAndUpstreamRoutesDenyBearerBeforeMutation(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	iamSvc := &routeIAMService{}
	mountSvc := &routeIAMMountSAService{}
	admin.RegisterIAMOnly(h, &admin.Deps{
		IAM:        iamSvc,
		IAMPolicy:  &fakePolicyService{},
		IAMMountSA: mountSvc,
		ActorAuth:  &actorAuthStub{principal: principal.OIDC("https://idp.example.com/", "alice", "oidc:example:alice", nil)},
		AdminAuthz: &credentialAuthorizerStub{decision: policy.DecisionDeny, reason: "implicit Deny"},
	})
	start()

	doRouteTestRequestAuth(t, http.MethodPost, base+"/v1/iam/mount-sa", "Bearer route-token", bytes.NewBufferString(`{"name":"mount","uid":1000}`), http.StatusForbidden)
	doRouteTestRequestAuth(t, http.MethodPut, base+"/v1/upstreams", "Bearer route-token", bytes.NewBufferString(`{"bucket":"logs","upstream_url":"https://s3.example.com","access_key":"AKIA","secret_key":"secret"}`), http.StatusForbidden)

	require.Empty(t, mountSvc.proposed)
	require.Empty(t, iamSvc.upstreamPuts)
}

func TestIAMMountSAAndUpstreamRoutesBearerFailsClosedWithoutAuthorizer(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	iamSvc := &routeIAMService{}
	mountSvc := &routeIAMMountSAService{}
	admin.RegisterIAMOnly(h, &admin.Deps{
		IAM:        iamSvc,
		IAMMountSA: mountSvc,
		ActorAuth:  &actorAuthStub{principal: principal.OIDC("https://idp.example.com/", "alice", "oidc:example:alice", nil)},
	})
	start()

	doRouteTestRequestAuth(t, http.MethodGet, base+"/v1/iam/mount-sa", "Bearer route-token", nil, http.StatusForbidden)
	doRouteTestRequestAuth(t, http.MethodGet, base+"/v1/upstreams", "Bearer route-token", nil, http.StatusForbidden)

	require.Empty(t, mountSvc.proposed)
	require.Zero(t, iamSvc.upstreamLists)
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

func TestIAMMountSAAndUpstreamRoutesRejectMalformedBearerBeforeFallback(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	iamSvc := &routeIAMService{}
	mountSvc := &routeIAMMountSAService{}
	authz := &credentialAuthorizerStub{decision: policy.DecisionAllow}
	admin.RegisterIAMOnly(h, &admin.Deps{
		IAM:        iamSvc,
		IAMMountSA: mountSvc,
		ActorAuth:  &actorAuthStub{principal: principal.OIDC("https://idp.example.com/", "alice", "oidc:example:alice", nil)},
		AdminAuthz: authz,
	})
	start()

	doRouteTestRequestAuth(t, http.MethodGet, base+"/v1/iam/mount-sa", "Bearer\tbad-token", nil, http.StatusUnauthorized)
	doRouteTestRequestAuth(t, http.MethodGet, base+"/v1/upstreams", "Bearer\tbad-token", nil, http.StatusUnauthorized)

	require.Empty(t, authz.calls)
	require.Zero(t, iamSvc.upstreamLists)
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

func TestIAMMountSAAndUpstreamRoutesWithoutBearerKeepUDSFallback(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	iamSvc := &routeIAMService{}
	mountSvc := &routeIAMMountSAService{}
	authz := &credentialAuthorizerStub{decision: policy.DecisionDeny}
	admin.RegisterIAMOnly(h, &admin.Deps{
		IAM:        iamSvc,
		IAMPolicy:  &fakePolicyService{},
		IAMMountSA: mountSvc,
		AdminAuthz: authz,
	})
	start()

	doRouteTestRequestAuth(t, http.MethodPost, base+"/v1/iam/mount-sa", "", bytes.NewBufferString(`{"name":"mount","uid":1000}`), http.StatusCreated)
	doRouteTestRequestAuth(t, http.MethodPut, base+"/v1/upstreams", "", bytes.NewBufferString(`{"bucket":"logs","upstream_url":"https://s3.example.com","access_key":"AKIA","secret_key":"secret"}`), http.StatusNoContent)

	require.Equal(t, []clusterpb.MetaCmdType{clusterpb.MetaCmdTypeMountSACreate}, mountSvc.proposed)
	require.Len(t, iamSvc.upstreamPuts, 1)
	require.Empty(t, authz.calls)
}

func TestIAMMutationRoutesWithoutBearerKeepUDSFallback(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	policySvc := &fakePolicyService{}
	authz := &credentialAuthorizerStub{decision: policy.DecisionDeny}
	admin.RegisterIAMOnly(h, &admin.Deps{
		IAM:        &routeIAMService{},
		IAMPolicy:  policySvc,
		AdminAuthz: authz,
	})
	start()

	body := bytes.NewBufferString(`{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)
	doRouteTestRequestAuth(t, http.MethodPut, base+"/v1/iam/policy/storage-admin", "", body, http.StatusNoContent)

	require.Len(t, policySvc.proposed, 1)
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
