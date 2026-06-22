package policy

import (
	"context"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/iam/principal"
)

type fakeStore struct {
	saToPols     map[string][]string
	saToGroups   map[string][]string
	groupToPols  map[string][]string
	bucketPols   map[string]string
	docs         map[string]string
	resolveCount int
}

func (f *fakeStore) SAPolicies(_ context.Context, saID string) ([]string, error) {
	f.resolveCount++
	return f.saToPols[saID], nil
}

func (f *fakeStore) SAGroups(_ context.Context, saID string) ([]string, error) {
	return f.saToGroups[saID], nil
}

func (f *fakeStore) GroupPolicies(_ context.Context, group string) ([]string, error) {
	return f.groupToPols[group], nil
}

func (f *fakeStore) PolicyDoc(_ context.Context, name string) (*Document, error) {
	raw, ok := f.docs[name]
	if !ok {
		return nil, nil
	}
	return Parse([]byte(raw))
}

func (f *fakeStore) BucketPolicy(_ context.Context, bucket string) (*Document, error) {
	raw, ok := f.bucketPols[bucket]
	if !ok {
		return nil, nil
	}
	return Parse([]byte(raw))
}

func TestResolver_CachesUntilTTL(t *testing.T) {
	s := &fakeStore{
		saToPols: map[string][]string{"sa-1": {"readonly"}},
		docs:     map[string]string{"readonly": `{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`},
	}
	r := NewResolver(s, 100*time.Millisecond)
	if _, err := r.Effective(context.Background(), "sa-1", "bucket-x", PrincipalTypeS3); err != nil {
		t.Fatalf("Effective#1: %v", err)
	}
	if _, err := r.Effective(context.Background(), "sa-1", "bucket-x", PrincipalTypeS3); err != nil {
		t.Fatalf("Effective#2: %v", err)
	}
	if s.resolveCount > 1 {
		t.Fatalf("cache miss on second call: resolveCount=%d", s.resolveCount)
	}
	time.Sleep(150 * time.Millisecond)
	if _, err := r.Effective(context.Background(), "sa-1", "bucket-x", PrincipalTypeS3); err != nil {
		t.Fatalf("Effective#3: %v", err)
	}
	if s.resolveCount != 2 {
		t.Fatalf("TTL did not expire: resolveCount=%d", s.resolveCount)
	}
}

func TestResolver_HasBucketPolicy(t *testing.T) {
	s := &fakeStore{
		bucketPols: map[string]string{
			"explicit": `{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::explicit/*"}]}`,
		},
	}
	r := NewResolver(s, time.Hour)
	has, err := r.HasBucketPolicy(context.Background(), "explicit")
	if err != nil || !has {
		t.Fatalf("explicit: has=%v err=%v", has, err)
	}
	has, err = r.HasBucketPolicy(context.Background(), "no-such")
	if err != nil || has {
		t.Fatalf("no-such: has=%v err=%v", has, err)
	}
}

func TestResolver_InvalidateClearsImmediately(t *testing.T) {
	s := &fakeStore{
		saToPols: map[string][]string{"sa-1": {"readonly"}},
		docs:     map[string]string{"readonly": `{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`},
	}
	r := NewResolver(s, 1*time.Hour)
	if _, err := r.Effective(context.Background(), "sa-1", "bucket-x", PrincipalTypeS3); err != nil {
		t.Fatal(err)
	}
	before := s.resolveCount
	r.Invalidate([]string{"sa-1"}, nil)
	if _, err := r.Effective(context.Background(), "sa-1", "bucket-x", PrincipalTypeS3); err != nil {
		t.Fatal(err)
	}
	if s.resolveCount != before+1 {
		t.Fatalf("invalidate did not force re-resolve: before=%d after=%d", before, s.resolveCount)
	}
}

func TestResolver_EffectivePrincipalOIDCDirectAndGroupPolicies(t *testing.T) {
	s := &fakeStore{
		saToPols:    map[string][]string{"oidc:issuer:user": {"direct-read"}},
		groupToPols: map[string][]string{"oidc:example:data-eng": {"group-write"}},
		docs: map[string]string{
			"direct-read": `{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`,
			"group-write": `{"Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"*"}]}`,
		},
	}
	r := NewResolver(s, time.Hour)
	p := principal.OIDC("https://idp.example.com/", "user", "oidc:issuer:user", []string{"oidc:example:data-eng"})

	in, err := r.EffectivePrincipal(context.Background(), p, "bucket-x")
	if err != nil {
		t.Fatalf("EffectivePrincipal: %v", err)
	}
	if in.Principal != "oidc:issuer:user" {
		t.Fatalf("principal = %q", in.Principal)
	}
	in.Ctx = RequestContext{Action: "s3:GetObject", Resource: "arn:aws:s3:::bucket-x/key"}
	if got := Evaluate(in); got.Decision != DecisionAllow || got.MatchedPolicy != "direct-read" {
		t.Fatalf("direct policy decision = %+v", got)
	}
	in.Ctx = RequestContext{Action: "s3:PutObject", Resource: "arn:aws:s3:::bucket-x/key"}
	if got := Evaluate(in); got.Decision != DecisionAllow || got.MatchedPolicy != "group-write" {
		t.Fatalf("group policy decision = %+v", got)
	}
}

func TestResolver_EffectivePrincipalOIDCGroupClaimsPartitionCache(t *testing.T) {
	s := &fakeStore{
		groupToPols: map[string][]string{
			"oidc:example:data-eng": {"group-write"},
		},
		docs: map[string]string{
			"group-write": `{"Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"*"}]}`,
		},
	}
	r := NewResolver(s, time.Hour)
	withGroup := principal.OIDC("https://idp.example.com/", "user", "oidc:issuer:user", []string{"oidc:example:data-eng"})
	withoutGroup := principal.OIDC("https://idp.example.com/", "user", "oidc:issuer:user", nil)

	in, err := r.EffectivePrincipal(context.Background(), withGroup, "bucket-x")
	if err != nil {
		t.Fatalf("EffectivePrincipal(with group): %v", err)
	}
	in.Ctx = RequestContext{Action: "s3:PutObject", Resource: "arn:aws:s3:::bucket-x/key"}
	if got := Evaluate(in); got.Decision != DecisionAllow {
		t.Fatalf("with group decision = %+v", got)
	}

	in, err = r.EffectivePrincipal(context.Background(), withoutGroup, "bucket-x")
	if err != nil {
		t.Fatalf("EffectivePrincipal(without group): %v", err)
	}
	in.Ctx = RequestContext{Action: "s3:PutObject", Resource: "arn:aws:s3:::bucket-x/key"}
	if got := Evaluate(in); got.Decision != DecisionDeny {
		t.Fatalf("without group decision = %+v", got)
	}
}

func TestResolver_InvalidateOIDCPrincipalAndBucket(t *testing.T) {
	s := &fakeStore{
		saToPols:   map[string][]string{"oidc:issuer:user": {"direct-read"}},
		bucketPols: map[string]string{"bucket-x": `{"Statement":[{"Effect":"Allow","Principal":{"AWS":["oidc:issuer:user"]},"Action":"s3:DeleteObject","Resource":"*"}]}`},
		docs: map[string]string{
			"direct-read": `{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`,
			"direct-put":  `{"Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"*"}]}`,
		},
	}
	r := NewResolver(s, time.Hour)
	p := principal.OIDC("https://idp.example.com/", "user", "oidc:issuer:user", []string{"oidc:example:data-eng"})

	if _, err := r.EffectivePrincipal(context.Background(), p, "bucket-x"); err != nil {
		t.Fatalf("EffectivePrincipal#1: %v", err)
	}
	s.saToPols["oidc:issuer:user"] = []string{"direct-put"}
	r.Invalidate([]string{"oidc:issuer:user"}, nil)
	in, err := r.EffectivePrincipal(context.Background(), p, "bucket-x")
	if err != nil {
		t.Fatalf("EffectivePrincipal#2: %v", err)
	}
	in.Ctx = RequestContext{Action: "s3:PutObject", Resource: "arn:aws:s3:::bucket-x/key"}
	if got := Evaluate(in); got.Decision != DecisionAllow || got.MatchedPolicy != "direct-put" {
		t.Fatalf("principal invalidation decision = %+v", got)
	}

	s.bucketPols["bucket-x"] = `{"Statement":[{"Effect":"Allow","Principal":{"AWS":["oidc:issuer:user"]},"Action":"s3:ListBucket","Resource":"*"}]}`
	r.Invalidate(nil, []string{"bucket-x"})
	in, err = r.EffectivePrincipal(context.Background(), p, "bucket-x")
	if err != nil {
		t.Fatalf("EffectivePrincipal#3: %v", err)
	}
	in.Ctx = RequestContext{Action: "s3:ListBucket", Resource: "arn:aws:s3:::bucket-x"}
	if got := Evaluate(in); got.Decision != DecisionAllow {
		t.Fatalf("bucket invalidation decision = %+v", got)
	}
}

func TestResolver_EffectivePrincipalServiceAccountCompatibility(t *testing.T) {
	s := &fakeStore{
		saToGroups:  map[string][]string{"sa-1": {"admins"}},
		groupToPols: map[string][]string{"admins": {"readonly"}},
		docs:        map[string]string{"readonly": `{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`},
	}
	r := NewResolver(s, time.Hour)
	in, err := r.EffectivePrincipal(context.Background(), principal.ServiceAccount("sa-1"), "bucket-x")
	if err != nil {
		t.Fatalf("EffectivePrincipal: %v", err)
	}
	in.Ctx = RequestContext{Action: "s3:GetObject", Resource: "arn:aws:s3:::bucket-x/key"}
	if got := Evaluate(in); got.Decision != DecisionAllow || got.MatchedPolicy != "readonly" {
		t.Fatalf("service account compatibility decision = %+v", got)
	}
}
