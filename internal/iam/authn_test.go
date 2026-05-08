package iam

import (
	"context"
	"slices"
	"testing"
)

func TestIAMSecretLookup_HitMissRevoked(t *testing.T) {
	s := NewStore()
	s.applySACreate(ServiceAccount{ID: "sa-1"})
	s.applyKeyCreate(AccessKey{
		AccessKey: "AK1", SecretKey: "secret", SAID: "sa-1", Status: KeyStatusActive,
	})

	lk := NewSecretLookup(s)

	if got, ok := lk("AK1"); !ok || got != "secret" {
		t.Fatalf("hit: got=%q ok=%v, want secret/true", got, ok)
	}
	if _, ok := lk("missing"); ok {
		t.Fatal("miss returned ok=true")
	}

	s.applyKeyRevoke("AK1")
	if _, ok := lk("AK1"); ok {
		t.Fatal("revoked key returned ok=true")
	}
}

func TestResolveSA_PopulatesPrincipal(t *testing.T) {
	s := NewStore()
	s.applySACreate(ServiceAccount{ID: "sa-1", Name: "alice"})
	s.applyKeyCreate(AccessKey{AccessKey: "AK1", SAID: "sa-1", Status: KeyStatusActive})

	saID, ok := ResolveSA(s, "AK1")
	if !ok || saID != "sa-1" {
		t.Fatalf("ResolveSA = %q/%v, want sa-1/true", saID, ok)
	}
	if _, ok := ResolveSA(s, "missing"); ok {
		t.Fatal("ResolveSA on missing returned ok=true")
	}
}

func TestPrincipalContext_RoundTrip(t *testing.T) {
	ctx := context.Background()
	if got := PrincipalFromContext(ctx); got != "" {
		t.Fatalf("empty context returned %q, want empty", got)
	}
	ctx2 := WithPrincipal(ctx, "sa-alice")
	if got := PrincipalFromContext(ctx2); got != "sa-alice" {
		t.Fatalf("with-principal returned %q, want sa-alice", got)
	}
	// Original ctx is not mutated
	if got := PrincipalFromContext(ctx); got != "" {
		t.Fatalf("parent context mutated: got %q", got)
	}
}

func TestWithPrincipalScope_Roundtrip(t *testing.T) {
	ctx := context.Background()
	ctx = WithPrincipal(ctx, "sa-alice")
	ctx = WithPrincipalScope(ctx, []string{"logs", "reports"})
	if got := PrincipalFromContext(ctx); got != "sa-alice" {
		t.Fatalf("PrincipalFromContext = %q, want sa-alice", got)
	}
	if got := ScopeFromContext(ctx); !slices.Equal(got, []string{"logs", "reports"}) {
		t.Fatalf("ScopeFromContext = %v, want [logs reports]", got)
	}
}

func TestScopeFromContext_Empty(t *testing.T) {
	if got := ScopeFromContext(context.Background()); got != nil {
		t.Fatalf("empty ctx ScopeFromContext = %v, want nil", got)
	}
}

func TestWithPrincipalScope_NilRoundtrip(t *testing.T) {
	ctx := WithPrincipalScope(context.Background(), nil)
	if got := ScopeFromContext(ctx); got != nil {
		t.Fatalf("nil-scope ctx should roundtrip to nil, got %v", got)
	}
}
