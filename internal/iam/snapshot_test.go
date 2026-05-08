package iam

import (
	"bytes"
	"slices"
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

func TestSnapshot_Version3_HeaderByte(t *testing.T) {
	store := NewStore()
	var buf bytes.Buffer
	if err := WriteSnapshot(&buf, store); err != nil {
		t.Fatalf("write: %v", err)
	}
	b := buf.Bytes()
	if len(b) == 0 || b[0] != 3 {
		t.Fatalf("first byte = %d, want 3", b[0])
	}
}

func TestSnapshot_BucketUpstream_TrailerAppendRoundtrip(t *testing.T) {
	src := NewStore()
	enc := newTestEncryptor(t)

	// A2: AAD = "bucket-upstream:"+bucket
	wrapped1, _ := WrapSecret(enc, "bucket-upstream:shared", "secret-A")
	wrapped2, _ := WrapSecret(enc, "bucket-upstream:archive", "secret-B")
	src.applyBucketUpstreamPut(BucketUpstream{
		Bucket: "shared", Endpoint: "http://up1:9000", AccessKey: "AK1",
		SecretKey: "secret-A", SecretKeyEnc: wrapped1,
		CreatedAt: time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC),
		CreatedBy: "sa-admin",
	})
	src.applyBucketUpstreamPut(BucketUpstream{
		Bucket: "archive", Endpoint: "http://up2:9000", AccessKey: "AK2",
		SecretKey: "secret-B", SecretKeyEnc: wrapped2,
		CreatedAt: time.Date(2026, 5, 8, 1, 0, 0, 0, time.UTC),
		CreatedBy: "sa-admin",
	})

	var buf bytes.Buffer
	if err := WriteSnapshot(&buf, src); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}

	dst := NewStore()
	if err := ReadSnapshot(bytes.NewReader(buf.Bytes()), dst, enc); err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}

	for _, want := range []struct {
		bucket, ak, sk, endpoint string
	}{
		{"shared", "AK1", "secret-A", "http://up1:9000"},
		{"archive", "AK2", "secret-B", "http://up2:9000"},
	} {
		got, ok := dst.LookupBucketUpstream(want.bucket)
		if !ok {
			t.Fatalf("LookupBucketUpstream(%s) missing after restore", want.bucket)
		}
		if got.AccessKey != want.ak || got.SecretKey != want.sk || got.Endpoint != want.endpoint {
			t.Errorf("restored %s: got %+v want ak=%s sk=%s endpoint=%s",
				want.bucket, got, want.ak, want.sk, want.endpoint)
		}
	}
}

// TestSnapshot_PreTrailerCompat_NoBucketUpstreamSection verifies that a snapshot
// emitted before the bucket-upstreams trailer is appended (5 sections + EOF)
// is still readable. This guards the A1 backward-compat property: v3 emitters
// without the trailer remain readable by v3 readers WITH the trailer logic.
func TestSnapshot_PreTrailerCompat_NoBucketUpstreamSection(t *testing.T) {
	// Construct a manual snapshot with only the existing 5 sections (sas, keys,
	// grants, wildcards, revoked), all empty.
	var buf bytes.Buffer
	buf.WriteByte(3) // version
	for i := 0; i < 5; i++ {
		var u32 [4]byte // each section: count = 0
		buf.Write(u32[:])
	}
	dst := NewStore()
	enc := newTestEncryptor(t)
	if err := ReadSnapshot(bytes.NewReader(buf.Bytes()), dst, enc); err != nil {
		t.Fatalf("ReadSnapshot v3 (pre-trailer): %v", err)
	}
}

// TestSnapshot_PostTrailerReadsForward verifies a snapshot WITH bucket-upstreams
// trailer is readable.
func TestSnapshot_PostTrailerReadsForward(t *testing.T) {
	src := NewStore()
	enc := newTestEncryptor(t)
	wrapped, _ := WrapSecret(enc, "bucket-upstream:b1", "s1")
	src.applyBucketUpstreamPut(BucketUpstream{
		Bucket: "b1", Endpoint: "http://x", AccessKey: "AK", SecretKeyEnc: wrapped,
	})

	var buf bytes.Buffer
	if err := WriteSnapshot(&buf, src); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}

	dst := NewStore()
	if err := ReadSnapshot(bytes.NewReader(buf.Bytes()), dst, enc); err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}
	if _, ok := dst.LookupBucketUpstream("b1"); !ok {
		t.Fatal("LookupBucketUpstream(b1) missing after restore")
	}
}
