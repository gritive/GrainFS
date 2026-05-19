package iam

import (
	"slices"
	"strings"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/gritive/GrainFS/internal/iam/iampb"
)

func buildSACreate(t *testing.T, saID, name string, ts time.Time) []byte {
	t.Helper()
	b := flatbuffers.NewBuilder(64)
	idOff := b.CreateString(saID)
	nameOff := b.CreateString(name)
	descOff := b.CreateString("")
	createdByOff := b.CreateString("")
	iampb.SACreatePayloadStart(b)
	iampb.SACreatePayloadAddSaId(b, idOff)
	iampb.SACreatePayloadAddName(b, nameOff)
	iampb.SACreatePayloadAddDescription(b, descOff)
	iampb.SACreatePayloadAddCreatedAtUnixNs(b, ts.UnixNano())
	iampb.SACreatePayloadAddCreatedBy(b, createdByOff)
	end := iampb.SACreatePayloadEnd(b)
	b.Finish(end)
	return b.FinishedBytes()
}

func buildSADelete(t *testing.T, saID string) []byte {
	t.Helper()
	b := flatbuffers.NewBuilder(32)
	idOff := b.CreateString(saID)
	iampb.SADeletePayloadStart(b)
	iampb.SADeletePayloadAddSaId(b, idOff)
	end := iampb.SADeletePayloadEnd(b)
	b.Finish(end)
	return b.FinishedBytes()
}

func buildKeyCreate(t *testing.T, ak, saID string, encBytes []byte, ts time.Time, expires int64) []byte {
	t.Helper()
	b := flatbuffers.NewBuilder(128)
	akOff := b.CreateString(ak)
	saOff := b.CreateString(saID)
	encOff := b.CreateByteVector(encBytes)
	iampb.KeyCreatePayloadStart(b)
	iampb.KeyCreatePayloadAddAccessKey(b, akOff)
	iampb.KeyCreatePayloadAddSecretKeyEnc(b, encOff)
	iampb.KeyCreatePayloadAddSaId(b, saOff)
	iampb.KeyCreatePayloadAddCreatedAtUnixNs(b, ts.UnixNano())
	iampb.KeyCreatePayloadAddExpiresAtUnixNs(b, expires)
	end := iampb.KeyCreatePayloadEnd(b)
	b.Finish(end)
	return b.FinishedBytes()
}

func buildKeyRevoke(t *testing.T, ak string) []byte {
	t.Helper()
	b := flatbuffers.NewBuilder(32)
	akOff := b.CreateString(ak)
	iampb.KeyRevokePayloadStart(b)
	iampb.KeyRevokePayloadAddAccessKey(b, akOff)
	end := iampb.KeyRevokePayloadEnd(b)
	b.Finish(end)
	return b.FinishedBytes()
}

func TestApplier_SACreate(t *testing.T) {
	s := NewStore()
	ap := NewApplier(s, newTestEncryptor(t))

	now := time.Unix(1700000000, 0)
	if err := ap.ApplySACreate(buildSACreate(t, "sa-1", "alice", now)); err != nil {
		t.Fatalf("ApplySACreate: %v", err)
	}
	sa, ok := s.LookupSA("sa-1")
	if !ok {
		t.Fatal("LookupSA miss after apply")
	}
	if sa.Name != "alice" {
		t.Fatalf("sa.Name = %q, want alice", sa.Name)
	}
}

func TestApplier_SACreate_Idempotent(t *testing.T) {
	s := NewStore()
	ap := NewApplier(s, newTestEncryptor(t))
	payload := buildSACreate(t, "sa-1", "alice", time.Unix(1, 0))

	if err := ap.ApplySACreate(payload); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if err := ap.ApplySACreate(payload); err != nil {
		t.Fatalf("second apply (idempotent): %v", err)
	}
}

func TestApplier_SACreate_EmptySaID(t *testing.T) {
	s := NewStore()
	ap := NewApplier(s, newTestEncryptor(t))
	if err := ap.ApplySACreate(buildSACreate(t, "", "alice", time.Unix(1, 0))); err == nil {
		t.Fatal("expected error for empty sa_id, got nil")
	}
}

func TestApplier_SADelete(t *testing.T) {
	s := NewStore()
	ap := NewApplier(s, newTestEncryptor(t))
	_ = ap.ApplySACreate(buildSACreate(t, "sa-1", "alice", time.Unix(1, 0)))
	if err := ap.ApplySADelete(buildSADelete(t, "sa-1")); err != nil {
		t.Fatalf("ApplySADelete: %v", err)
	}
	if _, ok := s.LookupSA("sa-1"); ok {
		t.Fatal("SA still present after delete")
	}
}

func TestApplier_KeyCreate_DecryptsSecret(t *testing.T) {
	enc := newTestEncryptor(t)
	s := NewStore()
	ap := NewApplier(s, enc)

	_ = ap.ApplySACreate(buildSACreate(t, "sa-1", "alice", time.Unix(1, 0)))

	wrapped, err := WrapSecret(enc, "sa-1", "secret-alice")
	if err != nil {
		t.Fatalf("WrapSecret: %v", err)
	}
	if err := ap.ApplyKeyCreate(buildKeyCreate(t, "AK1", "sa-1", wrapped, time.Unix(2, 0), 0)); err != nil {
		t.Fatalf("ApplyKeyCreate: %v", err)
	}
	k, ok := s.LookupKey("AK1")
	if !ok {
		t.Fatal("LookupKey miss after KeyCreate apply")
	}
	if k.SecretKey != "secret-alice" {
		t.Fatalf("SecretKey = %q, want secret-alice", k.SecretKey)
	}
}

func TestApplier_KeyCreate_AADMismatchFails(t *testing.T) {
	enc := newTestEncryptor(t)
	s := NewStore()
	ap := NewApplier(s, enc)

	// SA must exist so the apply path reaches the decrypt step.
	_ = ap.ApplySACreate(buildSACreate(t, "sa-B", "bob", time.Unix(1, 0)))
	wrappedForA, _ := WrapSecret(enc, "sa-A", "secret")
	if err := ap.ApplyKeyCreate(buildKeyCreate(t, "AK1", "sa-B", wrappedForA, time.Unix(2, 0), 0)); err == nil {
		t.Fatal("expected AAD mismatch error, got nil")
	}
}

func TestApplier_KeyRevoke(t *testing.T) {
	enc := newTestEncryptor(t)
	s := NewStore()
	ap := NewApplier(s, enc)
	// SA must exist so the key is actually stored before revoking.
	_ = ap.ApplySACreate(buildSACreate(t, "sa-1", "alice", time.Unix(1, 0)))
	wrapped, _ := WrapSecret(enc, "sa-1", "secret")
	_ = ap.ApplyKeyCreate(buildKeyCreate(t, "AK1", "sa-1", wrapped, time.Unix(2, 0), 0))
	if err := ap.ApplyKeyRevoke(buildKeyRevoke(t, "AK1")); err != nil {
		t.Fatalf("ApplyKeyRevoke: %v", err)
	}
	if _, ok := s.LookupKey("AK1"); ok {
		t.Fatal("revoked key still resolves")
	}
}

// buildKeyCreateScoped builds a KeyCreatePayload FlatBuffer with a bucket_scope vector.
func buildKeyCreateScoped(t *testing.T, ak, saID string, encBytes []byte, ts time.Time, expires int64, scope []string) []byte {
	t.Helper()
	b := flatbuffers.NewBuilder(256)
	// Pre-create all strings/vectors (must be done before StartObject).
	akOff := b.CreateString(ak)
	saOff := b.CreateString(saID)
	encOff := b.CreateByteVector(encBytes)
	// Build scope vector of strings.
	scopeOffsets := make([]flatbuffers.UOffsetT, len(scope))
	for i, s := range scope {
		scopeOffsets[i] = b.CreateString(s)
	}
	iampb.KeyCreatePayloadStartBucketScopeVector(b, len(scope))
	for i := len(scopeOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(scopeOffsets[i])
	}
	scopeVec := b.EndVector(len(scope))
	iampb.KeyCreatePayloadStart(b)
	iampb.KeyCreatePayloadAddAccessKey(b, akOff)
	iampb.KeyCreatePayloadAddSecretKeyEnc(b, encOff)
	iampb.KeyCreatePayloadAddSaId(b, saOff)
	iampb.KeyCreatePayloadAddCreatedAtUnixNs(b, ts.UnixNano())
	iampb.KeyCreatePayloadAddExpiresAtUnixNs(b, expires)
	iampb.KeyCreatePayloadAddBucketScope(b, scopeVec)
	end := iampb.KeyCreatePayloadEnd(b)
	b.Finish(end)
	return b.FinishedBytes()
}

func TestApplyKeyCreateScoped_Happy(t *testing.T) {
	enc := newTestEncryptor(t)
	s := NewStore()
	ap := NewApplier(s, enc)

	_ = ap.ApplySACreate(buildSACreate(t, "sa-1", "alice", time.Unix(1, 0)))

	wrapped, err := WrapSecret(enc, "sa-1", "secret")
	if err != nil {
		t.Fatalf("WrapSecret: %v", err)
	}
	payload := buildKeyCreateScoped(t, "AK1", "sa-1", wrapped, time.Unix(2, 0), 0, []string{"logs"})
	if err := ap.ApplyKeyCreateScoped(payload); err != nil {
		t.Fatalf("apply err: %v", err)
	}
	got, ok := s.LookupKey("AK1")
	if !ok {
		t.Fatal("key not persisted")
	}
	if !slices.Equal(got.BucketScope, []string{"logs"}) {
		t.Fatalf("scope = %v, want [logs]", got.BucketScope)
	}
}

func TestApplyKeyCreate_LegacyType23_NilScope(t *testing.T) {
	enc := newTestEncryptor(t)
	s := NewStore()
	ap := NewApplier(s, enc)

	_ = ap.ApplySACreate(buildSACreate(t, "sa-1", "alice", time.Unix(1, 0)))

	wrapped, _ := WrapSecret(enc, "sa-1", "secret")
	// Use legacy buildKeyCreate (no scope field)
	payload := buildKeyCreate(t, "AK_LEGACY", "sa-1", wrapped, time.Unix(2, 0), 0)
	if err := ap.ApplyKeyCreate(payload); err != nil {
		t.Fatalf("apply err: %v", err)
	}
	got, ok := s.LookupKey("AK_LEGACY")
	if !ok {
		t.Fatal("key not persisted")
	}
	if got.BucketScope != nil {
		t.Fatalf("legacy path scope = %v, want nil", got.BucketScope)
	}
}

func TestApplyBucketUpstreamPut_RoundTripDecryptsSecret(t *testing.T) {
	s := NewStore()
	enc := newTestEncryptor(t)
	ap := NewApplier(s, enc)

	// A2: AAD prefix is "bucket-upstream:" + bucket
	wrapped, err := WrapSecret(enc, "bucket-upstream:shared", "upstream-secret-plain")
	if err != nil {
		t.Fatalf("WrapSecret: %v", err)
	}
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	u := BucketUpstream{
		Bucket: "shared", Endpoint: "http://up.example:9000",
		AccessKey: "AKUP", SecretKeyEnc: wrapped,
		CreatedAt: now, CreatedBy: "sa-admin",
	}
	if err := ap.ApplyBucketUpstreamPut(buildBucketUpstreamPutPayload(u)); err != nil {
		t.Fatalf("ApplyBucketUpstreamPut: %v", err)
	}

	got, ok := s.LookupBucketUpstream("shared")
	if !ok {
		t.Fatal("LookupBucketUpstream(shared) returned ok=false after Apply")
	}
	if got.SecretKey != "upstream-secret-plain" {
		t.Errorf("decrypted SecretKey: got %q want %q", got.SecretKey, "upstream-secret-plain")
	}
	if got.AccessKey != "AKUP" || got.Endpoint != "http://up.example:9000" {
		t.Errorf("scalar fields mismatch: got %+v", got)
	}
	if got.CreatedAt.UnixNano() != now.UnixNano() {
		t.Errorf("CreatedAt: got %v want %v", got.CreatedAt, now)
	}
}

func TestApplyBucketUpstreamPut_RoundTripsStatus(t *testing.T) {
	s := NewStore()
	enc := newTestEncryptor(t)
	ap := NewApplier(s, enc)
	wrapped, err := WrapSecret(enc, "bucket-upstream:shared", "sk")
	if err != nil {
		t.Fatalf("WrapSecret: %v", err)
	}
	u := BucketUpstream{
		Bucket:       "shared",
		Endpoint:     "http://minio:9000",
		AccessKey:    "ak",
		SecretKeyEnc: wrapped,
		CreatedAt:    time.Unix(100, 0),
		CreatedBy:    "admin",
		Status:       BucketUpstreamStatusCutover,
	}
	if err := ap.ApplyBucketUpstreamPut(buildBucketUpstreamPutPayload(u)); err != nil {
		t.Fatalf("ApplyBucketUpstreamPut: %v", err)
	}
	got, ok := s.LookupBucketUpstream("shared")
	if !ok {
		t.Fatal("LookupBucketUpstream(shared) returned ok=false after Apply")
	}
	if got.Status != BucketUpstreamStatusCutover {
		t.Fatalf("Status = %q, want %q", got.Status, BucketUpstreamStatusCutover)
	}
}

func TestApplyBucketUpstreamStatusSet(t *testing.T) {
	s := NewStore()
	enc := newTestEncryptor(t)
	ap := NewApplier(s, enc)
	wrapped, err := WrapSecret(enc, "bucket-upstream:shared", "sk")
	if err != nil {
		t.Fatalf("WrapSecret: %v", err)
	}
	u := BucketUpstream{
		Bucket:       "shared",
		Endpoint:     "http://minio:9000",
		AccessKey:    "ak",
		SecretKeyEnc: wrapped,
		CreatedAt:    time.Unix(100, 0),
		CreatedBy:    "admin",
		Status:       BucketUpstreamStatusActive,
	}
	if err := ap.ApplyBucketUpstreamPut(buildBucketUpstreamPutPayload(u)); err != nil {
		t.Fatalf("ApplyBucketUpstreamPut: %v", err)
	}
	if err := ap.ApplyBucketUpstreamStatusSet("shared", BucketUpstreamStatusCutover); err != nil {
		t.Fatalf("ApplyBucketUpstreamStatusSet: %v", err)
	}
	got, ok := s.LookupBucketUpstream("shared")
	if !ok {
		t.Fatal("LookupBucketUpstream(shared) returned ok=false after status set")
	}
	if got.Status != BucketUpstreamStatusCutover {
		t.Fatalf("Status = %q, want %q", got.Status, BucketUpstreamStatusCutover)
	}
}

func TestApplyBucketUpstreamDelete_Idempotent(t *testing.T) {
	s := NewStore()
	enc := newTestEncryptor(t)
	ap := NewApplier(s, enc)

	if err := ap.ApplyBucketUpstreamDelete(buildBucketUpstreamDeletePayload("ghost")); err != nil {
		t.Fatalf("ApplyBucketUpstreamDelete on empty store: %v", err)
	}

	wrapped, _ := WrapSecret(enc, "bucket-upstream:buc1", "s")
	if err := ap.ApplyBucketUpstreamPut(buildBucketUpstreamPutPayload(BucketUpstream{
		Bucket: "buc1", Endpoint: "http://x", AccessKey: "AK", SecretKeyEnc: wrapped,
	})); err != nil {
		t.Fatalf("seed Apply: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := ap.ApplyBucketUpstreamDelete(buildBucketUpstreamDeletePayload("buc1")); err != nil {
			t.Fatalf("ApplyBucketUpstreamDelete iter %d: %v", i, err)
		}
	}
	if _, ok := s.LookupBucketUpstream("buc1"); ok {
		t.Fatal("LookupBucketUpstream(b1) returned ok=true after delete")
	}
}

func TestApplyBucketUpstreamPut_RejectsEmptyBucket(t *testing.T) {
	s := NewStore()
	enc := newTestEncryptor(t)
	ap := NewApplier(s, enc)

	wrapped, _ := WrapSecret(enc, "bucket-upstream:", "s")
	err := ap.ApplyBucketUpstreamPut(buildBucketUpstreamPutPayload(BucketUpstream{
		Bucket: "", Endpoint: "http://x", AccessKey: "AK", SecretKeyEnc: wrapped,
	}))
	if err == nil {
		t.Fatal("ApplyBucketUpstreamPut with empty bucket: want error, got nil")
	}
}

// Per A7(c): Sentinel bucket reject test.
func TestApplyBucketUpstreamPut_RejectsSentinelBuckets(t *testing.T) {
	s := NewStore()
	enc := newTestEncryptor(t)
	ap := NewApplier(s, enc)

	for _, sentinel := range []string{"*", "__system__"} {
		wrapped, _ := WrapSecret(enc, "bucket-upstream:"+sentinel, "s")
		err := ap.ApplyBucketUpstreamPut(buildBucketUpstreamPutPayload(BucketUpstream{
			Bucket: sentinel, Endpoint: "http://x", AccessKey: "AK", SecretKeyEnc: wrapped,
		}))
		if err == nil {
			t.Errorf("ApplyBucketUpstreamPut with sentinel %q: want error, got nil", sentinel)
		}
	}
}

// Per A7(b): wrong-AAD decrypt failure test.
func TestApplyBucketUpstreamPut_WrongAADFailsDecrypt(t *testing.T) {
	s := NewStore()
	enc := newTestEncryptor(t)
	ap := NewApplier(s, enc)

	// Wrap with WRONG AAD (using bare bucket name without prefix — this should fail at apply).
	wrapped, _ := WrapSecret(enc, "shared", "secret")

	err := ap.ApplyBucketUpstreamPut(buildBucketUpstreamPutPayload(BucketUpstream{
		Bucket: "shared", Endpoint: "http://x", AccessKey: "AK", SecretKeyEnc: wrapped,
	}))
	if err == nil {
		t.Fatal("ApplyBucketUpstreamPut with wrong-AAD ciphertext: want error, got nil")
	}
	// Tighten: must specifically be the decrypt path, not validation rejection.
	if !strings.Contains(err.Error(), "decrypt") {
		t.Fatalf("expected decrypt-path error, got: %v", err)
	}
}
