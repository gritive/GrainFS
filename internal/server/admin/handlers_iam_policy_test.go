package admin_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/server/admin"
)

// fakePolicyService satisfies admin.IAMPolicyService without any real storage.
type fakePolicyService struct {
	proposed []clusterpb.MetaCmdType
	simReqs  []admin.PolicySimulateRequest
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

func TestPutPolicy_BuiltinRefused(t *testing.T) {
	d := &admin.Deps{IAMPolicy: &fakePolicyService{}}
	doc := []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)

	for _, name := range []string{"readonly", "readwrite", "writeonly", "bucket-admin"} {
		err := admin.PutPolicy(context.Background(), d, name, doc)
		if err == nil {
			t.Fatalf("PutPolicy(%q): expected error, got nil", name)
		}
		var ae *admin.Error
		if !errors.As(err, &ae) {
			t.Fatalf("PutPolicy(%q): expected *admin.Error, got %T: %v", name, err, err)
		}
		if ae.Code != "forbidden" {
			t.Fatalf("PutPolicy(%q): expected code=forbidden, got %q", name, ae.Code)
		}
	}
}

func TestPutPolicy_CustomNameAllowed(t *testing.T) {
	svc := &fakePolicyService{}
	d := &admin.Deps{IAMPolicy: svc}
	doc := []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)

	if err := admin.PutPolicy(context.Background(), d, "my-custom-pol", doc); err != nil {
		t.Fatalf("PutPolicy(custom): unexpected error: %v", err)
	}
	if len(svc.proposed) != 1 {
		t.Fatalf("expected 1 Propose call, got %d", len(svc.proposed))
	}
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
	if err != nil {
		t.Fatalf("SimulatePolicy: %v", err)
	}
	if len(svc.simReqs) != 1 || svc.simReqs[0].SAID != "sa-1" {
		t.Fatalf("legacy request not delegated: %#v", svc.simReqs)
	}
}

func assertAdminCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error")
	}
	var ae *admin.Error
	if !errors.As(err, &ae) {
		t.Fatalf("expected *admin.Error, got %T: %v", err, err)
	}
	if ae.Code != code {
		t.Fatalf("code = %q, want %q", ae.Code, code)
	}
}
