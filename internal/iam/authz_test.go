package iam

import (
	"testing"

	"github.com/gritive/GrainFS/internal/s3auth"
)

func TestCheckAccess_ExplicitGrant(t *testing.T) {
	s := NewStore()
	s.applySACreate(ServiceAccount{ID: "sa-alice"})
	s.applyGrantPut(Grant{SAID: "sa-alice", Bucket: "logs", Role: RoleWrite})

	if !CheckAccess(s, "sa-alice", "logs", s3auth.PutObject) {
		t.Error("Write role should allow PutObject on owned bucket")
	}
	if CheckAccess(s, "sa-alice", "logs", s3auth.CreateBucket) {
		t.Error("Write role should NOT allow CreateBucket")
	}
	if CheckAccess(s, "sa-alice", "other-bucket", s3auth.GetObject) {
		t.Error("no grant on other-bucket should deny")
	}
}

func TestCheckAccess_WildcardFallback(t *testing.T) {
	s := NewStore()
	s.applySACreate(ServiceAccount{ID: "sa-default"})
	s.applyGrantWildcardPut(Grant{SAID: "sa-default", Role: RoleAdmin})

	if !CheckAccess(s, "sa-default", "any-bucket", s3auth.CreateBucket) {
		t.Error("wildcard Admin should allow CreateBucket on any bucket")
	}
	if !CheckAccess(s, "sa-default", "another-bucket", s3auth.GetObject) {
		t.Error("wildcard Admin should allow GetObject on any bucket")
	}
}

func TestCheckAccess_ExplicitOverridesWildcard(t *testing.T) {
	s := NewStore()
	s.applySACreate(ServiceAccount{ID: "sa-default"})
	s.applyGrantWildcardPut(Grant{SAID: "sa-default", Role: RoleAdmin})
	s.applyGrantPut(Grant{SAID: "sa-default", Bucket: "readonly", Role: RoleRead})

	// Explicit Read on "readonly" must override wildcard Admin
	if CheckAccess(s, "sa-default", "readonly", s3auth.PutObject) {
		t.Error("explicit Read on bucket must override wildcard Admin (least-privilege wins on explicit)")
	}
	if !CheckAccess(s, "sa-default", "readonly", s3auth.GetObject) {
		t.Error("explicit Read should still allow GetObject")
	}
	// Other buckets fall back to wildcard
	if !CheckAccess(s, "sa-default", "anywhere-else", s3auth.PutObject) {
		t.Error("buckets without explicit grant should fall back to wildcard")
	}
}

func TestCheckAccess_NoSA(t *testing.T) {
	s := NewStore()
	if CheckAccess(s, "sa-missing", "logs", s3auth.GetObject) {
		t.Error("missing SA must deny")
	}
}

func TestCheckAccess_EmptySA(t *testing.T) {
	s := NewStore()
	s.applyGrantWildcardPut(Grant{SAID: "sa-default", Role: RoleAdmin})
	if CheckAccess(s, "", "any", s3auth.GetObject) {
		t.Error("empty saID must deny")
	}
}
