package protocred

import (
	"crypto/sha256"
	"errors"
	"testing"
	"time"
)

func TestStoreApplyCreateIsIdempotentForIdenticalRow(t *testing.T) {
	store := NewStore()
	row := testApplyCredential("pc_apply")

	first, err := store.ApplyCreate(row)
	if err != nil {
		t.Fatalf("ApplyCreate first: %v", err)
	}
	second, err := store.ApplyCreate(row)
	if err != nil {
		t.Fatalf("ApplyCreate replay: %v", err)
	}
	if first.ID != second.ID || len(store.Snapshot()) != 1 {
		t.Fatalf("ApplyCreate replay created duplicate: first=%+v second=%+v rows=%d", first, second, len(store.Snapshot()))
	}
}

func TestStoreApplyCreateRejectsConflictingRow(t *testing.T) {
	store := NewStore()
	row := testApplyCredential("pc_apply")
	if _, err := store.ApplyCreate(row); err != nil {
		t.Fatalf("ApplyCreate first: %v", err)
	}
	row.SecretHint = "different"
	if _, err := store.ApplyCreate(row); !errors.Is(err, ErrConflict) {
		t.Fatalf("ApplyCreate conflict err = %v, want %v", err, ErrConflict)
	}
}

func TestStoreApplyRotateRevokeStaleAndLastUsed(t *testing.T) {
	store := NewStore()
	row := testApplyCredential("pc_apply")
	if _, err := store.ApplyCreate(row); err != nil {
		t.Fatalf("ApplyCreate: %v", err)
	}

	newHash := sha256.Sum256([]byte("rotated"))
	rotated, err := store.ApplyRotate(row.ID, newHash, "rotated-hint")
	if err != nil {
		t.Fatalf("ApplyRotate: %v", err)
	}
	if rotated.SecretHash != newHash || rotated.SecretHint != "rotated-hint" || rotated.Generation != 2 {
		t.Fatalf("rotated = %+v", rotated)
	}
	rotated, err = store.ApplyRotateWithSecretEnc(row.ID, sha256.Sum256([]byte("rotated-enc")), "rotated-enc-hint", []byte("sealed-rotated"))
	if err != nil {
		t.Fatalf("ApplyRotateWithSecretEnc: %v", err)
	}
	if string(rotated.SecretEnc) != "sealed-rotated" || rotated.SecretHint != "rotated-enc-hint" || rotated.Generation != 3 {
		t.Fatalf("rotated with enc = %+v", rotated)
	}

	used := row.CreatedAt.Add(2 * time.Hour)
	if _, err := store.ApplyLastUsed(row.ID, used); err != nil {
		t.Fatalf("ApplyLastUsed newer: %v", err)
	}
	if _, err := store.ApplyLastUsed(row.ID, used.Add(-time.Hour)); err != nil {
		t.Fatalf("ApplyLastUsed older: %v", err)
	}
	got, err := NewService(store).Get(row.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.LastUsedAt == nil || !got.LastUsedAt.Equal(used) {
		t.Fatalf("LastUsedAt = %v, want %s", got.LastUsedAt, used)
	}

	staleAt := used.Add(time.Hour)
	if _, err := store.ApplyMarkStale(row.ID, staleAt, "policy_detached"); err != nil {
		t.Fatalf("ApplyMarkStale first: %v", err)
	}
	if _, err := store.ApplyMarkStale(row.ID, staleAt.Add(time.Hour), "policy_changed"); err != nil {
		t.Fatalf("ApplyMarkStale replay: %v", err)
	}
	got, err = NewService(store).Get(row.ID)
	if err != nil {
		t.Fatalf("Get stale: %v", err)
	}
	if got.StaleAt == nil || !got.StaleAt.Equal(staleAt) || got.StaleReason != "policy_detached" {
		t.Fatalf("stale = %v/%q, want %s/policy_detached", got.StaleAt, got.StaleReason, staleAt)
	}

	revokedAt := staleAt.Add(time.Hour)
	if _, err := store.ApplyRevoke(row.ID, revokedAt); err != nil {
		t.Fatalf("ApplyRevoke first: %v", err)
	}
	if _, err := store.ApplyRevoke(row.ID, revokedAt.Add(time.Hour)); err != nil {
		t.Fatalf("ApplyRevoke replay: %v", err)
	}
	got, err = NewService(store).Get(row.ID)
	if err != nil {
		t.Fatalf("Get revoked: %v", err)
	}
	if got.RevokedAt == nil || !got.RevokedAt.Equal(revokedAt) {
		t.Fatalf("RevokedAt = %v, want %s", got.RevokedAt, revokedAt)
	}
	if _, err := store.ApplyRotate(row.ID, sha256.Sum256([]byte("after-revoke")), "after"); !errors.Is(err, ErrRevoked) {
		t.Fatalf("ApplyRotate after revoke err = %v, want %v", err, ErrRevoked)
	}
}

func TestStoreApplyMissingCredentialErrors(t *testing.T) {
	store := NewStore()
	now := time.Date(2026, 5, 28, 1, 2, 3, 0, time.UTC)
	if _, err := store.ApplyRotate("missing", sha256.Sum256([]byte("x")), "hint"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ApplyRotate missing err = %v, want %v", err, ErrNotFound)
	}
	if _, err := store.ApplyRevoke("missing", now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ApplyRevoke missing err = %v, want %v", err, ErrNotFound)
	}
	if _, err := store.ApplyMarkStale("missing", now, "reason"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ApplyMarkStale missing err = %v, want %v", err, ErrNotFound)
	}
	if _, err := store.ApplyLastUsed("missing", now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ApplyLastUsed missing err = %v, want %v", err, ErrNotFound)
	}
}

func testApplyCredential(id string) Credential {
	return Credential{
		ID:         id,
		SAID:       "sa_apply",
		Protocol:   ProtocolNFS,
		Resource:   "volume/apply",
		Mode:       ModeRW,
		SecretHash: sha256.Sum256([]byte("secret")),
		SecretHint: "secret-hint",
		CreatedAt:  time.Date(2026, 5, 28, 1, 2, 3, 0, time.UTC),
		CreatedBy:  "admin",
		Generation: 1,
	}
}
