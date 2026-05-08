package iam

import (
	"testing"
	"time"
)

func TestStore_EmptyReads(t *testing.T) {
	s := NewStore()

	if _, ok := s.LookupKey("missing"); ok {
		t.Fatal("LookupKey on empty store returned ok")
	}
	if r := s.LookupGrant("sa-1", "bucket-1"); r != RoleNone {
		t.Fatalf("LookupGrant on empty store = %v, want RoleNone", r)
	}
}

func TestStore_PutSAAndKey_Read(t *testing.T) {
	s := NewStore()
	now := time.Unix(1700000000, 0)

	s.applySACreate(ServiceAccount{ID: "sa-1", Name: "alice", CreatedAt: now})
	s.applyKeyCreate(AccessKey{
		AccessKey:    "AKIA-ALICE",
		SecretKey:    "secret-alice",
		SecretKeyEnc: []byte("ignored-in-test"),
		SAID:         "sa-1",
		Status:       KeyStatusActive,
		CreatedAt:    now,
	})

	k, ok := s.LookupKey("AKIA-ALICE")
	if !ok {
		t.Fatal("LookupKey miss for inserted key")
	}
	if k.SAID != "sa-1" {
		t.Fatalf("k.SAID = %q, want sa-1", k.SAID)
	}
	if k.SecretKey != "secret-alice" {
		t.Fatalf("k.SecretKey = %q, want secret-alice", k.SecretKey)
	}
}

func TestStore_GrantAndWildcard(t *testing.T) {
	s := NewStore()
	s.applyGrantPut(Grant{SAID: "sa-1", Bucket: "logs", Role: RoleRead})
	s.applyGrantWildcardPut(Grant{SAID: "sa-default", Role: RoleAdmin})

	if r := s.LookupGrant("sa-1", "logs"); r != RoleRead {
		t.Fatalf("explicit grant = %v, want RoleRead", r)
	}
	if r := s.LookupGrant("sa-1", "missing"); r != RoleNone {
		t.Fatalf("missing grant = %v, want RoleNone", r)
	}
	if r := s.LookupGrant("sa-default", "any-bucket"); r != RoleAdmin {
		t.Fatalf("wildcard fallback = %v, want RoleAdmin", r)
	}
}

func TestStore_KeyRevoke_LookupReturnsNotOk(t *testing.T) {
	s := NewStore()
	s.applyKeyCreate(AccessKey{AccessKey: "AK", SAID: "sa-1", Status: KeyStatusActive})
	s.applyKeyRevoke("AK")

	if _, ok := s.LookupKey("AK"); ok {
		t.Fatal("LookupKey returned ok after revoke")
	}
}

func TestStore_KeyExpired_LookupReturnsNotOk(t *testing.T) {
	s := NewStore()
	past := time.Now().Add(-time.Hour)
	s.applyKeyCreate(AccessKey{AccessKey: "AK", SAID: "sa-1", Status: KeyStatusActive, ExpiresAt: &past})
	if _, ok := s.LookupKey("AK"); ok {
		t.Fatal("LookupKey returned ok for expired key")
	}
}
