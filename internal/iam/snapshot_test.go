package iam

import (
	"bytes"
	"slices"
	"strings"
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
}

func TestSnapshot_PreservesBucketScope(t *testing.T) {
	enc := newTestEncryptor(t)
	src := NewStore()
	src.applySACreate(ServiceAccount{ID: "sa-1", Name: "alice", CreatedAt: time.Unix(1, 0)})
	src.applyGrantPut(Grant{SAID: "sa-1", Bucket: "logs", Role: RoleRead})
	wrapped, err := WrapSecret(enc, "sa-1", "secret-scoped")
	if err != nil {
		t.Fatalf("WrapSecret: %v", err)
	}
	src.applyKeyCreate(AccessKey{
		AccessKey:    "AK_S",
		SecretKey:    "secret-scoped",
		SecretKeyEnc: wrapped,
		SAID:         "sa-1",
		Status:       KeyStatusActive,
		CreatedAt:    time.Unix(2, 0),
		BucketScope:  []string{"logs"},
	})

	var buf bytes.Buffer
	if err := WriteSnapshot(&buf, src); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}

	dst := NewStore()
	if err := ReadSnapshot(&buf, dst, enc); err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}
	got, ok := dst.LookupKey("AK_S")
	if !ok {
		t.Fatal("key not restored after snapshot round-trip")
	}
	if !slices.Equal(got.BucketScope, []string{"logs"}) {
		t.Fatalf("scope = %v, want [logs]", got.BucketScope)
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

func TestSnapshot_Version2_HeaderByte(t *testing.T) {
	store := NewStore()
	var buf bytes.Buffer
	if err := WriteSnapshot(&buf, store); err != nil {
		t.Fatalf("write: %v", err)
	}
	b := buf.Bytes()
	if len(b) == 0 || b[0] != 2 {
		t.Fatalf("first byte = %d, want 2", b[0])
	}
}

func TestSnapshot_V1_StillReadable(t *testing.T) {
	// Hand-craft a minimal v1 blob: header byte 1, authBit 0, four zero u32 counts.
	// Format: [u8 version=1][u8 authBit=0][u32 nSA=0][u32 nKeys=0][u32 nGrants=0][u32 nWildcards=0]
	// Note: nRevoked section is optional (EOF = 0).
	buf := []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	r := bytes.NewReader(buf)
	enc := newTestEncryptor(t)
	dst := NewStore()
	if err := ReadSnapshot(r, dst, enc); err != nil {
		t.Fatalf("v1 read should succeed (forward compat), got %v", err)
	}
}

func TestSnapshot_V3_Rejected(t *testing.T) {
	buf := []byte{3, 0}
	r := bytes.NewReader(buf)
	enc := newTestEncryptor(t)
	dst := NewStore()
	err := ReadSnapshot(r, dst, enc)
	if err == nil {
		t.Fatal("expected version error, got nil")
	}
	if !strings.Contains(err.Error(), "version") {
		t.Fatalf("err = %v, expected to mention 'version'", err)
	}
}
