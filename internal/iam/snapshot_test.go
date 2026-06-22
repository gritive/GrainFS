package iam

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
)

func TestSnapshot_Roundtrip(t *testing.T) {
	enc := newTestEncryptor(t)
	src := NewStore()
	src.applySACreate(ServiceAccount{ID: "sa-1", Name: "alice", CreatedAt: time.Unix(1, 0)})
	wrapped, gen, _ := WrapSecret(enc, "sa-1", "AK1", "secret-alice")
	src.applyKeyCreate(AccessKey{
		AccessKey: "AK1", SecretKey: "secret-alice", SecretKeyEnc: wrapped, SecretKeyDEKGen: gen,
		SAID: "sa-1", Status: KeyStatusActive, CreatedAt: time.Unix(2, 0),
	})

	var buf bytes.Buffer
	if err := WriteSnapshot(&buf, src); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}

	dst := NewStore()
	if err := ReadSnapshot(&buf, dst, enc); err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}

	k, ok := dst.LookupKey("AK1")
	if !ok {
		t.Fatal("LookupKey miss after restore")
	}
	if k.SecretKey != "secret-alice" {
		t.Fatalf("secret after restore = %q, want secret-alice", k.SecretKey)
	}
	sa, ok := dst.LookupSA("sa-1")
	if !ok {
		t.Fatal("LookupSA miss after restore")
	}
	if sa.Name != "alice" {
		t.Fatalf("sa.Name after restore = %q, want alice", sa.Name)
	}
}

func TestSnapshot_SecretKeyNotInPlaintextOnDisk(t *testing.T) {
	enc := newTestEncryptor(t)
	src := NewStore()
	wrapped, gen, _ := WrapSecret(enc, "sa-1", "AK", "very-secret-token-xyz")
	src.applySACreate(ServiceAccount{ID: "sa-1"})
	src.applyKeyCreate(AccessKey{
		AccessKey: "AK", SAID: "sa-1",
		SecretKey: "very-secret-token-xyz", SecretKeyEnc: wrapped, SecretKeyDEKGen: gen,
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
	wrapped, gen, err := WrapSecret(enc, "sa-1", "AK_S", "secret-scoped")
	if err != nil {
		t.Fatalf("WrapSecret: %v", err)
	}
	src.applyKeyCreate(AccessKey{
		AccessKey:       "AK_S",
		SecretKey:       "secret-scoped",
		SecretKeyEnc:    wrapped,
		SecretKeyDEKGen: gen,
		SAID:            "sa-1",
		Status:          KeyStatusActive,
		CreatedAt:       time.Unix(2, 0),
		BucketScope:     []string{"logs"},
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
	wrappedLive, genLive, _ := WrapSecret(enc, "sa-1", "AK-LIVE", "secret")
	wrappedRev, genRev, _ := WrapSecret(enc, "sa-1", "AK-REVOKED", "secret")
	src.applySACreate(ServiceAccount{ID: "sa-1"})
	src.applyKeyCreate(AccessKey{
		AccessKey: "AK-LIVE", SAID: "sa-1", SecretKey: "secret",
		SecretKeyEnc: wrappedLive, SecretKeyDEKGen: genLive, Status: KeyStatusActive,
	})
	src.applyKeyCreate(AccessKey{
		AccessKey: "AK-REVOKED", SAID: "sa-1", SecretKey: "secret",
		SecretKeyEnc: wrappedRev, SecretKeyDEKGen: genRev, Status: KeyStatusActive,
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

func TestSnapshot_Version4_HeaderByte(t *testing.T) {
	store := NewStore()
	var buf bytes.Buffer
	if err := WriteSnapshot(&buf, store); err != nil {
		t.Fatalf("write: %v", err)
	}
	b := buf.Bytes()
	if len(b) == 0 || b[0] != 4 {
		t.Fatalf("first byte = %d, want 4 (v4)", b[0])
	}
}

func TestSnapshot_BucketUpstream_TrailerAppendRoundtrip(t *testing.T) {
	src := NewStore()
	enc := newTestEncryptor(t)

	// A2: AAD = "bucket-upstream:"+bucket; codex P2 binds access_key too.
	wrapped1, gen1, _ := WrapSecret(enc, "bucket-upstream:shared", "AK1", "secret-A")
	wrapped2, gen2, _ := WrapSecret(enc, "bucket-upstream:archive", "AK2", "secret-B")
	src.applyBucketUpstreamPut(BucketUpstream{
		Bucket: "shared", Endpoint: "http://up1:9000", AccessKey: "AK1",
		SecretKey: "secret-A", SecretKeyEnc: wrapped1, SecretKeyDEKGen: gen1,
		CreatedAt: time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC),
		CreatedBy: "sa-admin",
	})
	src.applyBucketUpstreamPut(BucketUpstream{
		Bucket: "archive", Endpoint: "http://up2:9000", AccessKey: "AK2",
		SecretKey: "secret-B", SecretKeyEnc: wrapped2, SecretKeyDEKGen: gen2,
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

// TestSnapshot_PostTrailerReadsForward verifies a snapshot WITH bucket-upstreams
// section is readable.
func TestSnapshot_PostTrailerReadsForward(t *testing.T) {
	src := NewStore()
	enc := newTestEncryptor(t)
	wrapped, gen, _ := WrapSecret(enc, "bucket-upstream:buc1", "AK", "s1")
	src.applyBucketUpstreamPut(BucketUpstream{
		Bucket: "buc1", Endpoint: "http://x", AccessKey: "AK", SecretKeyEnc: wrapped, SecretKeyDEKGen: gen,
	})

	var buf bytes.Buffer
	if err := WriteSnapshot(&buf, src); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}

	dst := NewStore()
	if err := ReadSnapshot(bytes.NewReader(buf.Bytes()), dst, enc); err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}
	if _, ok := dst.LookupBucketUpstream("buc1"); !ok {
		t.Fatal("LookupBucketUpstream(b1) missing after restore")
	}
}

// fakeDEKEncryptor wraps a real DataEncryptor and returns a CALLER-FIXED gen
// on Seal, so this round-trip test proves the gen really survives via the
// payload bytes (not by accident through the inner adapter's active gen 0).
type fakeDEKEncryptor struct {
	inner storage.DataEncryptor
	gen   uint32
}

func (f *fakeDEKEncryptor) Seal(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	ct, _, err := f.inner.Seal(domain, fields, plain)
	return ct, f.gen, err
}

func (f *fakeDEKEncryptor) SealTo(_ []byte, domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	return f.Seal(domain, fields, plain)
}

func (f *fakeDEKEncryptor) SealAtGen(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte, _ uint32) ([]byte, error) {
	ct, _, err := f.Seal(domain, fields, plain)
	return ct, err
}

func (f *fakeDEKEncryptor) SealAtGenTo(_ []byte, domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte, _ uint32) ([]byte, error) {
	ct, _, err := f.Seal(domain, fields, plain)
	return ct, err
}

func (f *fakeDEKEncryptor) Open(domain encrypt.AADDomain, fields []encrypt.AADField, gen uint32, ct []byte) ([]byte, error) {
	if gen != f.gen {
		return nil, fmt.Errorf("fakeDEKEncryptor: expected gen %d, got %d", f.gen, gen)
	}
	return f.inner.Open(domain, fields, 0, ct)
}

func (f *fakeDEKEncryptor) OpenTo(dst []byte, domain encrypt.AADDomain, fields []encrypt.AADField, gen uint32, ct []byte) ([]byte, error) {
	if gen != f.gen {
		return nil, fmt.Errorf("fakeDEKEncryptor: expected gen %d, got %d", f.gen, gen)
	}
	return f.inner.OpenTo(dst, domain, fields, 0, ct)
}

func TestSnapshot_DEKGenSurvivesAccessKeyRoundTrip(t *testing.T) {
	de := &fakeDEKEncryptor{inner: staticTestEncryptor(t), gen: 7}

	src := NewStore()
	ct, gen, err := WrapSecret(de, "sa-1", "AKIA-1", "secret123")
	require.NoError(t, err)
	require.Equal(t, uint32(7), gen, "fake returns non-zero gen")

	src.applyKeyCreate(AccessKey{
		AccessKey:       "AKIA-1",
		SecretKey:       "secret123",
		SecretKeyEnc:    ct,
		SecretKeyDEKGen: gen,
		SAID:            "sa-1",
		Status:          KeyStatusActive,
	})

	var buf bytes.Buffer
	require.NoError(t, WriteSnapshot(&buf, src))

	dst := NewStore()
	require.NoError(t, ReadSnapshot(&buf, dst, de))

	k, ok := dst.LookupKey("AKIA-1")
	require.True(t, ok)
	require.Equal(t, uint32(7), k.SecretKeyDEKGen, "gen survives FB payload + Apply path")
	require.Equal(t, "secret123", k.SecretKey, "in-memory plaintext rehydrated via UnwrapSecret")
}

func TestSnapshot_DEKGenSurvivesBucketUpstreamRoundTrip(t *testing.T) {
	de := &fakeDEKEncryptor{inner: staticTestEncryptor(t), gen: 11}

	src := NewStore()
	bucketSAID := "bucket-upstream:my-bucket"
	ct, gen, err := WrapSecret(de, bucketSAID, "AK1", "upstream-secret")
	require.NoError(t, err)

	src.applyBucketUpstreamPut(BucketUpstream{
		Bucket:          "my-bucket",
		Endpoint:        "http://example",
		AccessKey:       "AK1",
		SecretKey:       "upstream-secret",
		SecretKeyEnc:    ct,
		SecretKeyDEKGen: gen,
		Status:          BucketUpstreamStatusActive,
	})

	var buf bytes.Buffer
	require.NoError(t, WriteSnapshot(&buf, src))

	dst := NewStore()
	require.NoError(t, ReadSnapshot(&buf, dst, de))

	u, ok := dst.LookupBucketUpstream("my-bucket")
	require.True(t, ok)
	require.Equal(t, uint32(11), u.SecretKeyDEKGen)
	require.Equal(t, "upstream-secret", u.SecretKey)
}

// The CAS generation must survive a snapshot write/read round-trip. Two puts
// drive Generation to 2 via the live apply path; after WriteSnapshot →
// ReadSnapshot the restored store must report Generation 2, not reset to 1.
// This FAILS if ReadSnapshot routes through ApplyBucketUpstreamPut (live,
// recompute) instead of ApplyBucketUpstreamPutFromSnapshot (verbatim).
func TestSnapshot_GenerationSurvivesBucketUpstreamRoundTrip(t *testing.T) {
	enc := newTestEncryptor(t)

	src := NewStore()
	ap := NewApplier(src, enc)
	wrapped, _, err := WrapSecret(enc, "bucket-upstream:my-bucket", "AK1", "upstream-secret")
	require.NoError(t, err)
	put := buildBucketUpstreamPutPayload(BucketUpstream{
		Bucket:       "my-bucket",
		Endpoint:     "http://example",
		AccessKey:    "AK1",
		SecretKeyEnc: wrapped,
		Status:       BucketUpstreamStatusActive,
	})
	require.NoError(t, ap.ApplyBucketUpstreamPut(put)) // gen 1
	require.NoError(t, ap.ApplyBucketUpstreamPut(put)) // gen 2
	pre, ok := src.LookupBucketUpstream("my-bucket")
	require.True(t, ok)
	require.Equal(t, uint64(2), pre.Generation, "second live put → gen 2")

	var buf bytes.Buffer
	require.NoError(t, WriteSnapshot(&buf, src))

	dst := NewStore()
	require.NoError(t, ReadSnapshot(&buf, dst, enc))

	u, ok := dst.LookupBucketUpstream("my-bucket")
	require.True(t, ok)
	require.Equal(t, uint64(2), u.Generation, "generation survives snapshot round-trip (not reset to 1)")
}
