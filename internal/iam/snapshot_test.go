package iam

import (
	"bytes"
	"testing"
	"time"
)

func TestSnapshot_Roundtrip(t *testing.T) {
	enc := newTestEncryptor(t)
	src := NewStore()
	src.applySACreate(ServiceAccount{ID: "sa-1", Name: "alice", CreatedAt: time.Unix(1, 0)})
	wrapped, _ := WrapSecret(enc, "sa-1", "secret-alice")
	src.applyKeyCreate(AccessKey{
		AccessKey: "AK1", SecretKey: "secret-alice", SecretKeyEnc: wrapped,
		SAID: "sa-1", Status: KeyStatusActive, CreatedAt: time.Unix(2, 0),
	})
	src.applyGrantPut(Grant{SAID: "sa-1", Bucket: "logs", Role: RoleWrite})
	src.applyGrantWildcardPut(Grant{SAID: "sa-default", Role: RoleAdmin})
	src.applyAuthEnable()

	var buf bytes.Buffer
	if err := WriteSnapshot(&buf, src); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}

	dst := NewStore()
	if err := ReadSnapshot(&buf, dst, enc); err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}

	if got := dst.LookupGrant("sa-1", "logs"); got != RoleWrite {
		t.Fatalf("grant after restore = %v, want RoleWrite", got)
	}
	if got := dst.LookupGrant("sa-default", "any"); got != RoleAdmin {
		t.Fatalf("wildcard after restore = %v, want RoleAdmin", got)
	}
	if !dst.AuthEnabled() {
		t.Fatal("authEnabled lost after restore")
	}
	k, ok := dst.LookupKey("AK1")
	if !ok {
		t.Fatal("LookupKey miss after restore")
	}
	if k.SecretKey != "secret-alice" {
		t.Fatalf("secret after restore = %q, want secret-alice", k.SecretKey)
	}
}

func TestSnapshot_SecretKeyNotInPlaintextOnDisk(t *testing.T) {
	enc := newTestEncryptor(t)
	src := NewStore()
	wrapped, _ := WrapSecret(enc, "sa-1", "very-secret-token-xyz")
	src.applySACreate(ServiceAccount{ID: "sa-1"})
	src.applyKeyCreate(AccessKey{
		AccessKey: "AK", SAID: "sa-1",
		SecretKey: "very-secret-token-xyz", SecretKeyEnc: wrapped,
		Status: KeyStatusActive,
	})

	var buf bytes.Buffer
	if err := WriteSnapshot(&buf, src); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	if bytes.Contains(buf.Bytes(), []byte("very-secret-token-xyz")) {
		t.Fatal("plaintext secret found in snapshot bytes — encryption violated")
	}
}

func TestSnapshot_EmptyStoreRoundtrip(t *testing.T) {
	enc := newTestEncryptor(t)
	src := NewStore()
	var buf bytes.Buffer
	if err := WriteSnapshot(&buf, src); err != nil {
		t.Fatalf("WriteSnapshot empty: %v", err)
	}
	dst := NewStore()
	if err := ReadSnapshot(&buf, dst, enc); err != nil {
		t.Fatalf("ReadSnapshot empty: %v", err)
	}
	if !dst.IsEmpty() {
		t.Fatal("dst not empty after restoring empty snapshot")
	}
	if dst.AuthEnabled() {
		t.Fatal("authEnabled flipped on empty snapshot")
	}
}

func TestSnapshot_RevokedKeyStatusPreserved(t *testing.T) {
	enc := newTestEncryptor(t)
	src := NewStore()
	wrapped, _ := WrapSecret(enc, "sa-1", "secret")
	src.applySACreate(ServiceAccount{ID: "sa-1"})
	src.applyKeyCreate(AccessKey{
		AccessKey: "AK-LIVE", SAID: "sa-1", SecretKey: "secret",
		SecretKeyEnc: wrapped, Status: KeyStatusActive,
	})
	src.applyKeyCreate(AccessKey{
		AccessKey: "AK-REVOKED", SAID: "sa-1", SecretKey: "secret",
		SecretKeyEnc: wrapped, Status: KeyStatusActive,
	})
	src.applyKeyRevoke("AK-REVOKED")

	var buf bytes.Buffer
	if err := WriteSnapshot(&buf, src); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	dst := NewStore()
	if err := ReadSnapshot(&buf, dst, enc); err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}

	// Live key still resolves.
	if _, ok := dst.LookupKey("AK-LIVE"); !ok {
		t.Fatal("LookupKey miss for live key after restore")
	}
	// Revoked key MUST NOT resolve.
	if _, ok := dst.LookupKey("AK-REVOKED"); ok {
		t.Fatal("LookupKey hit for revoked key after restore — Status not preserved")
	}
}
