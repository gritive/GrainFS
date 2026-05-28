package serveruntime

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/server/admin"
)

func TestIAMPolicyAdminAdapter_SimulateOIDCGroupPolicy(t *testing.T) {
	ctx := context.Background()
	stores, err := WireIAMPolicyStores(ctx, nil, time.Hour)
	if err != nil {
		t.Fatalf("WireIAMPolicyStores: %v", err)
	}
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)
	if err := stores.Policies.Put(ctx, "oidc-read", doc, false); err != nil {
		t.Fatalf("Put policy: %v", err)
	}
	if err := stores.Attach.AttachToGroup(ctx, "oidc:example:data-eng", "oidc-read"); err != nil {
		t.Fatalf("AttachToGroup: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	if res.Effect != "Allow" || res.MatchedPolicy != "oidc-read" {
		t.Fatalf("result = %+v", res)
	}
	if res.PrincipalKind != "oidc" || res.PrincipalID == "" || len(res.Groups) != 1 {
		t.Fatalf("principal audit fields missing: %+v", res)
	}
}

func TestIAMPolicyAdminAdapter_RejectsOIDCPrincipalIDMismatch(t *testing.T) {
	ctx := context.Background()
	stores, err := WireIAMPolicyStores(ctx, nil, time.Hour)
	if err != nil {
		t.Fatalf("WireIAMPolicyStores: %v", err)
	}
	adapter := NewIAMPolicyAdminAdapter(stores, func(context.Context, clusterpb.MetaCmdType, []byte) error { return nil })
	_, err = adapter.Simulate(ctx, admin.PolicySimulateRequest{
		PrincipalKind: "oidc",
		PrincipalID:   "oidc:wrong",
		Issuer:        "https://idp.example.com/",
		Subject:       "user@example.com",
		Action:        "s3:GetObject",
		Resource:      "arn:aws:s3:::bucket/key",
	})
	if err == nil {
		t.Fatalf("expected mismatch error")
	}
	var ae *admin.Error
	if !errors.As(err, &ae) {
		t.Fatalf("expected admin error, got %T: %v", err, err)
	}
	if ae.Code != "invalid" {
		t.Fatalf("code = %q, want invalid", ae.Code)
	}
}

func testOIDCPrincipalID(issuer, subject string) string {
	issuerSum := sha256.Sum256([]byte(issuer))
	subjectSum := sha256.Sum256([]byte(subject))
	return fmt.Sprintf("oidc:%x:%x", issuerSum[:8], subjectSum[:16])
}
