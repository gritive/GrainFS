package admin_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/iam/principal"
	"github.com/gritive/GrainFS/internal/server/admin"
)

// fakePolicyService satisfies admin.IAMPolicyService without any real storage.
type fakePolicyService struct {
	proposed     []clusterpb.MetaCmdType
	simReqs      []admin.PolicySimulateRequest
	selfPolicies map[string]bool
	selfGroups   map[string]bool
}

func (f *fakePolicyService) Propose(_ context.Context, cmdType clusterpb.MetaCmdType, _ []byte) error {
	f.proposed = append(f.proposed, cmdType)
	return nil
}

func (f *fakePolicyService) PolicyDoc(_ context.Context, _ string) ([]byte, error) {
	return nil, nil
}

func (f *fakePolicyService) PolicyList(_ context.Context) ([]string, error) {
	return nil, nil
}

func (f *fakePolicyService) Simulate(_ context.Context, req admin.PolicySimulateRequest) (admin.PolicySimulateResult, error) {
	f.simReqs = append(f.simReqs, req)
	return admin.PolicySimulateResult{}, nil
}

func (f *fakePolicyService) PolicyAffectsPrincipal(_ context.Context, _ principal.Principal, policyName string) (bool, error) {
	return f.selfPolicies[policyName], nil
}

func (f *fakePolicyService) GroupAffectsPrincipal(_ context.Context, _ principal.Principal, group string) (bool, error) {
	return f.selfGroups[group], nil
}

func TestPutPolicy_BuiltinRefused(t *testing.T) {
	d := &admin.Deps{IAMPolicy: &fakePolicyService{}}
	doc := []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)

	for _, name := range []string{"readonly", "readwrite", "writeonly", "bucket-admin"} {
		err := admin.PutPolicy(context.Background(), d, name, doc)
		require.Error(t, err, "PutPolicy(%q)", name)
		var ae *admin.Error
		require.True(t, errors.As(err, &ae), "PutPolicy(%q): expected *admin.Error, got %T: %v", name, err, err)
		require.Equal(t, "forbidden", ae.Code, "PutPolicy(%q)", name)
	}
}

func TestPutPolicy_CustomNameAllowed(t *testing.T) {
	svc := &fakePolicyService{}
	d := &admin.Deps{IAMPolicy: svc}
	doc := []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)

	err := admin.PutPolicy(context.Background(), d, "my-custom-pol", doc)
	require.NoError(t, err)
	require.Len(t, svc.proposed, 1)
}

func TestSimulatePolicyRejectsMissingPrincipal(t *testing.T) {
	d := &admin.Deps{IAMPolicy: &fakePolicyService{}}
	_, err := admin.SimulatePolicy(context.Background(), d, admin.PolicySimulateRequest{
		Action:   "s3:GetObject",
		Resource: "arn:aws:s3:::bucket/key",
	})
	assertAdminCode(t, err, "invalid")
}

func TestSimulatePolicyRejectsAmbiguousPrincipal(t *testing.T) {
	d := &admin.Deps{IAMPolicy: &fakePolicyService{}}
	_, err := admin.SimulatePolicy(context.Background(), d, admin.PolicySimulateRequest{
		SAID:          "sa-1",
		PrincipalKind: "oidc",
		PrincipalID:   "oidc:issuer:user",
		Action:        "s3:GetObject",
		Resource:      "arn:aws:s3:::bucket/key",
	})
	assertAdminCode(t, err, "invalid")
}

func TestSimulatePolicyRejectsIncompletePrincipal(t *testing.T) {
	d := &admin.Deps{IAMPolicy: &fakePolicyService{}}
	_, err := admin.SimulatePolicy(context.Background(), d, admin.PolicySimulateRequest{
		PrincipalKind: "oidc",
		Action:        "s3:GetObject",
		Resource:      "arn:aws:s3:::bucket/key",
	})
	assertAdminCode(t, err, "invalid")
}

func TestSimulatePolicyRejectsUnsupportedPrincipalKind(t *testing.T) {
	d := &admin.Deps{IAMPolicy: &fakePolicyService{}}
	_, err := admin.SimulatePolicy(context.Background(), d, admin.PolicySimulateRequest{
		PrincipalKind: "user",
		PrincipalID:   "user-1",
		Action:        "s3:GetObject",
		Resource:      "arn:aws:s3:::bucket/key",
	})
	assertAdminCode(t, err, "invalid")
}

func TestSimulatePolicyPassesLegacySARequest(t *testing.T) {
	svc := &fakePolicyService{}
	d := &admin.Deps{IAMPolicy: svc}
	_, err := admin.SimulatePolicy(context.Background(), d, admin.PolicySimulateRequest{
		SAID:     "sa-1",
		Action:   "s3:GetObject",
		Resource: "arn:aws:s3:::bucket/key",
	})
	require.NoError(t, err)
	require.Len(t, svc.simReqs, 1)
	require.Equal(t, "sa-1", svc.simReqs[0].SAID)
}

func assertAdminCode(t *testing.T, err error, code string) {
	t.Helper()
	require.Error(t, err)
	var ae *admin.Error
	require.True(t, errors.As(err, &ae), "expected *admin.Error, got %T: %v", err, err)
	require.Equal(t, code, ae.Code)
}
