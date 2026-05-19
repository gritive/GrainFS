package s3auth

import (
	"context"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/iam/bucketpolicy"
	"github.com/gritive/GrainFS/internal/iam/group"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/iam/policyattach"
	"github.com/gritive/GrainFS/internal/iam/policystore"
)

type stubCfg struct{ vals map[string]bool }

func (s *stubCfg) GetBool(k string) (bool, bool) {
	v, ok := s.vals[k]
	return v, ok
}

func newTestAuthorizer(t *testing.T, anon, allowAnonBucket bool) (*Authorizer, *policystore.InMemoryStore, *policyattach.InMemoryStore, *bucketpolicy.InMemoryStore) {
	t.Helper()
	ps := policystore.NewInMemoryStore()
	gs := group.NewInMemoryStore()
	pa := policyattach.NewInMemoryStore()
	bp := bucketpolicy.NewInMemoryStore()
	adapter := &policy.StoreAdapter{Policies: ps, Attach: pa, Groups: gs, Buckets: bp}
	res := policy.NewResolver(adapter, 100*time.Millisecond)
	cfg := &stubCfg{vals: map[string]bool{
		"iam.anon-enabled":                  anon,
		"iam.allow-anonymous-bucket-policy": allowAnonBucket,
	}}
	return NewAuthorizer(res, cfg), ps, pa, bp
}

func TestAuthorize_AdminUDSOnlyDeniedOnDataPlane(t *testing.T) {
	a, _, _, _ := newTestAuthorizer(t, true, false)
	for _, act := range []string{"s3:CreateBucket", "s3:DeleteBucket", "s3:PutBucketPolicy", "s3:DeleteBucketPolicy"} {
		r := a.Authorize(context.Background(), "sa-1", "b", policy.RequestContext{Action: act, Resource: "arn:aws:s3:::b"})
		if r.Decision != policy.DecisionDeny {
			t.Errorf("%s should be denied on data plane, got %v", act, r.Decision)
		}
	}
}

func TestAuthorize_AnonInternalBucketDenied(t *testing.T) {
	a, _, _, _ := newTestAuthorizer(t, true /* anon-enabled */, false)
	r := a.Authorize(context.Background(), "" /* anon */, "_grainfs", policy.RequestContext{Action: "s3:GetObject", Resource: "arn:aws:s3:::_grainfs/x"})
	if r.Decision != policy.DecisionDeny {
		t.Fatalf("anon to _grainfs should be denied even with anon-enabled, got %v", r.Decision)
	}
}

func TestAuthorize_AnonEnabledShortCircuit(t *testing.T) {
	a, _, _, _ := newTestAuthorizer(t, true, false)
	r := a.Authorize(context.Background(), "", "userbucket", policy.RequestContext{Action: "s3:GetObject", Resource: "arn:aws:s3:::userbucket/x"})
	if r.Decision != policy.DecisionAllow {
		t.Fatalf("anon-enabled should Allow ordinary action, got %v: %s", r.Decision, r.Reason)
	}
}

func TestAuthorize_AnonDisabledNoBucketPolicy_Denied(t *testing.T) {
	a, _, _, _ := newTestAuthorizer(t, false, false)
	r := a.Authorize(context.Background(), "", "b", policy.RequestContext{Action: "s3:GetObject", Resource: "arn:aws:s3:::b/x"})
	if r.Decision != policy.DecisionDeny {
		t.Fatalf("anon-disabled + no bucket policy should Deny, got %v", r.Decision)
	}
}

func TestAuthorize_SAWithReadonlyPolicy(t *testing.T) {
	a, ps, pa, _ := newTestAuthorizer(t, false, false)
	_ = ps.Put(context.Background(), "readonly", []byte(`{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`), true)
	_ = pa.AttachToSA(context.Background(), "sa-1", "readonly")
	r := a.Authorize(context.Background(), "sa-1", "b", policy.RequestContext{Action: "s3:GetObject", Resource: "arn:aws:s3:::b/x"})
	if r.Decision != policy.DecisionAllow {
		t.Fatalf("sa-1 readonly should Allow GetObject, got %v: %s", r.Decision, r.Reason)
	}
}
