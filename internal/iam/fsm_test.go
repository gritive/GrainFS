package iam

import (
	"slices"
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

func buildGrantPut(t *testing.T, saID, bucket string, role Role, ts time.Time) []byte {
	t.Helper()
	b := flatbuffers.NewBuilder(64)
	saOff := b.CreateString(saID)
	bkOff := b.CreateString(bucket)
	cbOff := b.CreateString("")
	iampb.GrantPutPayloadStart(b)
	iampb.GrantPutPayloadAddSaId(b, saOff)
	iampb.GrantPutPayloadAddBucket(b, bkOff)
	iampb.GrantPutPayloadAddRole(b, iampb.Role(role))
	iampb.GrantPutPayloadAddCreatedAtUnixNs(b, ts.UnixNano())
	iampb.GrantPutPayloadAddCreatedBy(b, cbOff)
	end := iampb.GrantPutPayloadEnd(b)
	b.Finish(end)
	return b.FinishedBytes()
}

func buildGrantDelete(t *testing.T, saID, bucket string) []byte {
	t.Helper()
	b := flatbuffers.NewBuilder(64)
	saOff := b.CreateString(saID)
	bkOff := b.CreateString(bucket)
	iampb.GrantDeletePayloadStart(b)
	iampb.GrantDeletePayloadAddSaId(b, saOff)
	iampb.GrantDeletePayloadAddBucket(b, bkOff)
	end := iampb.GrantDeletePayloadEnd(b)
	b.Finish(end)
	return b.FinishedBytes()
}

func buildGrantWildcardDelete(t *testing.T, saID string) []byte {
	t.Helper()
	b := flatbuffers.NewBuilder(32)
	saOff := b.CreateString(saID)
	iampb.GrantWildcardDeletePayloadStart(b)
	iampb.GrantWildcardDeletePayloadAddSaId(b, saOff)
	end := iampb.GrantWildcardDeletePayloadEnd(b)
	b.Finish(end)
	return b.FinishedBytes()
}

func buildGrantWildcardPut(t *testing.T, saID string, role Role, ts time.Time) []byte {
	t.Helper()
	b := flatbuffers.NewBuilder(64)
	saOff := b.CreateString(saID)
	cbOff := b.CreateString("")
	iampb.GrantWildcardPutPayloadStart(b)
	iampb.GrantWildcardPutPayloadAddSaId(b, saOff)
	iampb.GrantWildcardPutPayloadAddRole(b, iampb.Role(role))
	iampb.GrantWildcardPutPayloadAddCreatedAtUnixNs(b, ts.UnixNano())
	iampb.GrantWildcardPutPayloadAddCreatedBy(b, cbOff)
	end := iampb.GrantWildcardPutPayloadEnd(b)
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

func TestApplier_GrantPut_RejectsWildcardBucket(t *testing.T) {
	s := NewStore()
	ap := NewApplier(s, newTestEncryptor(t))
	if err := ap.ApplyGrantPut(buildGrantPut(t, "sa-1", WildcardBucket, RoleAdmin, time.Unix(1, 0))); err == nil {
		t.Fatal("expected error for wildcard bucket via GrantPut, got nil")
	}
}

func TestApplier_GrantPut_Delete(t *testing.T) {
	s := NewStore()
	ap := NewApplier(s, newTestEncryptor(t))
	if err := ap.ApplyGrantPut(buildGrantPut(t, "sa-1", "logs", RoleWrite, time.Unix(1, 0))); err != nil {
		t.Fatalf("ApplyGrantPut: %v", err)
	}
	if got := s.LookupGrant("sa-1", "logs"); got != RoleWrite {
		t.Fatalf("after put: %v", got)
	}
	if err := ap.ApplyGrantDelete(buildGrantDelete(t, "sa-1", "logs")); err != nil {
		t.Fatalf("ApplyGrantDelete: %v", err)
	}
	if got := s.LookupGrant("sa-1", "logs"); got != RoleNone {
		t.Fatalf("after delete: %v, want RoleNone", got)
	}
}

func TestApplier_GrantWildcardPut(t *testing.T) {
	s := NewStore()
	ap := NewApplier(s, newTestEncryptor(t))
	if err := ap.ApplyGrantWildcardPut(buildGrantWildcardPut(t, "sa-default", RoleAdmin, time.Unix(1, 0))); err != nil {
		t.Fatalf("ApplyGrantWildcardPut: %v", err)
	}
	if got := s.LookupGrant("sa-default", "any-bucket"); got != RoleAdmin {
		t.Fatalf("wildcard fallback = %v, want RoleAdmin", got)
	}
}

func TestApplier_GrantWildcardDelete_RoundTrip(t *testing.T) {
	s := NewStore()
	ap := NewApplier(s, newTestEncryptor(t))
	// Use a non-default SA so the lockout-invariant guard in
	// ApplyGrantWildcardDelete (Phase 5d #3) doesn't apply — this test
	// exercises plain round-trip semantics, not the sa-default invariant.
	const saID = "sa-rt"
	if err := ap.ApplyGrantWildcardPut(buildGrantWildcardPut(t, saID, RoleAdmin, time.Unix(1, 0))); err != nil {
		t.Fatalf("ApplyGrantWildcardPut: %v", err)
	}
	if got := s.LookupGrant(saID, "any-bucket"); got != RoleAdmin {
		t.Fatalf("pre-delete fallback = %v, want RoleAdmin", got)
	}
	if err := ap.ApplyGrantWildcardDelete(buildGrantWildcardDelete(t, saID)); err != nil {
		t.Fatalf("ApplyGrantWildcardDelete: %v", err)
	}
	if got := s.LookupGrant(saID, "any-bucket"); got != RoleNone {
		t.Fatalf("post-delete = %v, want RoleNone", got)
	}
	// Idempotent on missing entry.
	if err := ap.ApplyGrantWildcardDelete(buildGrantWildcardDelete(t, saID)); err != nil {
		t.Fatalf("second ApplyGrantWildcardDelete: %v", err)
	}
}

// TestApplyGrantWildcardDelete_RejectsDefaultSALockout verifies that the
// FSM apply path silently no-ops a wildcard delete on sa-default when no
// explicit per-bucket grants exist — the lockout invariant. Pre-fix this
// check lived only in HandleGrantDelete; two concurrent admin clients
// could both pass the read-side guard and both propose, leaving zero
// grants on sa-default + sticky auth_enabled = cluster lockout.
func TestApplyGrantWildcardDelete_RejectsDefaultSALockout(t *testing.T) {
	s := NewStore()
	ap := NewApplier(s, newTestEncryptor(t))
	// Seed the wildcard so the would-be removal has something to remove.
	if err := ap.ApplyGrantWildcardPut(buildGrantWildcardPut(t, DefaultSAID, RoleAdmin, time.Unix(1, 0))); err != nil {
		t.Fatalf("ApplyGrantWildcardPut: %v", err)
	}
	if got := s.LookupGrant(DefaultSAID, "any"); got != RoleAdmin {
		t.Fatalf("pre-delete wildcard fallback = %v, want RoleAdmin", got)
	}
	// Apply must noop (return nil) but NOT remove the wildcard.
	if err := ap.ApplyGrantWildcardDelete(buildGrantWildcardDelete(t, DefaultSAID)); err != nil {
		t.Fatalf("ApplyGrantWildcardDelete: %v", err)
	}
	if got := s.LookupGrant(DefaultSAID, "any"); got != RoleAdmin {
		t.Fatalf("wildcard removed despite lockout invariant: got %v, want RoleAdmin", got)
	}

	// With at least one explicit grant present, removal is allowed.
	s.applyGrantPut(Grant{SAID: DefaultSAID, Bucket: "owned", Role: RoleAdmin})
	if err := ap.ApplyGrantWildcardDelete(buildGrantWildcardDelete(t, DefaultSAID)); err != nil {
		t.Fatalf("ApplyGrantWildcardDelete with explicit grant: %v", err)
	}
	if got := s.LookupGrant(DefaultSAID, "any"); got != RoleNone {
		t.Fatalf("post-delete wildcard fallback = %v, want RoleNone", got)
	}
	// Explicit grant survives.
	if got := s.LookupGrant(DefaultSAID, "owned"); got != RoleAdmin {
		t.Fatalf("explicit grant clobbered by wildcard delete: %v", got)
	}
}

func TestApplier_GrantWildcardDelete_EmptySAID(t *testing.T) {
	s := NewStore()
	ap := NewApplier(s, newTestEncryptor(t))
	if err := ap.ApplyGrantWildcardDelete(buildGrantWildcardDelete(t, "")); err == nil {
		t.Fatal("expected error for empty sa_id, got nil")
	}
}

func TestApplier_AuthEnable_Sticky(t *testing.T) {
	s := NewStore()
	ap := NewApplier(s, newTestEncryptor(t))
	if err := ap.ApplyAuthEnable(nil); err != nil {
		t.Fatalf("ApplyAuthEnable: %v", err)
	}
	if !s.AuthEnabled() {
		t.Fatal("AuthEnabled = false after apply")
	}
	if err := ap.ApplyAuthEnable(nil); err != nil {
		t.Fatalf("second ApplyAuthEnable: %v", err)
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
	_ = ap.ApplyGrantPut(buildGrantPut(t, "sa-1", "logs", RoleRead, time.Unix(1, 0)))

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

func TestApplyKeyCreateScoped_OverScope_Noop(t *testing.T) {
	enc := newTestEncryptor(t)
	s := NewStore()
	ap := NewApplier(s, enc)

	_ = ap.ApplySACreate(buildSACreate(t, "sa-1", "alice", time.Unix(1, 0)))
	_ = ap.ApplyGrantPut(buildGrantPut(t, "sa-1", "logs", RoleRead, time.Unix(1, 0)))

	wrapped, _ := WrapSecret(enc, "sa-1", "secret")
	// scope contains "reports" but SA has no grant on it
	payload := buildKeyCreateScoped(t, "AK_BAD", "sa-1", wrapped, time.Unix(2, 0), 0, []string{"logs", "reports"})
	if err := ap.ApplyKeyCreateScoped(payload); err != nil {
		t.Fatalf("over-scope should noop, got err %v (raft determinism requires nil)", err)
	}
	if _, ok := s.LookupKey("AK_BAD"); ok {
		t.Fatal("over-scope key must NOT be persisted")
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
