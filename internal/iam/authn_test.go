package iam

import (
	"context"
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
