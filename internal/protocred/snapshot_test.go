package protocred

import (
	"testing"
	"time"
)

func TestStoreSnapshotIsDeterministicAndDetached(t *testing.T) {
	now := time.Date(2026, 5, 28, 1, 2, 3, 4, time.UTC)
	exp := now.Add(time.Hour)
	revoked := now.Add(2 * time.Hour)
	used := now.Add(3 * time.Hour)

	store := NewStore()
	store.put(Credential{
		ID:         "pc_b",
		SAID:       "sa_b",
		Protocol:   ProtocolNFS,
		Resource:   "volume/b",
		Mode:       ModeRW,
		SecretHash: [32]byte{2},
		SecretHint: "pcsec_b",
		SecretEnc:  []byte("sealed-b"),
		CreatedAt:  now,
		CreatedBy:  "admin-b",
		ExpiresAt:  &exp,
		RevokedAt:  &revoked,
		LastUsedAt: &used,
	})
	store.put(Credential{
		ID:         "pc_a",
		SAID:       "sa_a",
		Protocol:   ProtocolS3,
		Resource:   "bucket/a",
		Mode:       ModeRO,
		SecretHash: [32]byte{1},
		SecretHint: "pcsec_a",
		CreatedAt:  now.Add(-time.Hour),
		CreatedBy:  "admin-a",
	})

	snap := store.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("Snapshot len = %d, want 2", len(snap))
	}
	if snap[0].ID != "pc_a" || snap[1].ID != "pc_b" {
		t.Fatalf("Snapshot order = [%s %s], want [pc_a pc_b]", snap[0].ID, snap[1].ID)
	}
	if snap[1].ExpiresAt == nil || snap[1].RevokedAt == nil || snap[1].LastUsedAt == nil {
		t.Fatalf("Snapshot lost pointer timestamps: %+v", snap[1])
	}

	*snap[1].ExpiresAt = now.Add(24 * time.Hour)
	snap[1].SecretHash[0] = 99
	snap[1].SecretEnc[0] = 'X'

	again := store.Snapshot()
	if !again[1].ExpiresAt.Equal(exp) {
		t.Fatalf("Snapshot did not detach ExpiresAt: got %s want %s", again[1].ExpiresAt, exp)
	}
	if again[1].SecretHash[0] != 2 {
		t.Fatalf("Snapshot did not detach SecretHash: got %d want 2", again[1].SecretHash[0])
	}
	if string(again[1].SecretEnc) != "sealed-b" {
		t.Fatalf("Snapshot did not detach SecretEnc: got %q want sealed-b", again[1].SecretEnc)
	}
}

func TestStoreRestoreIsDetachedAndPreservesNoPlaintextSecret(t *testing.T) {
	now := time.Date(2026, 5, 28, 4, 5, 6, 7, time.UTC)
	exp := now.Add(time.Hour)
	revoked := now.Add(2 * time.Hour)
	used := now.Add(3 * time.Hour)

	src := NewService(NewStore(), WithNow(func() time.Time { return now }))
	secret, err := src.Create(CreateRequest{
		SAID:      "sa_app",
		Protocol:  ProtocolNFS,
		Resource:  "volume/devdisk",
		Mode:      ModeRW,
		ExpiresAt: &exp,
		CreatedBy: "admin",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _ = src.store.update(secret.ID, func(item Credential) Credential {
		item.RevokedAt = &revoked
		item.LastUsedAt = &used
		return item
	})

	rows := src.store.Snapshot()
	dstStore := NewStore()
	dstStore.Restore(rows)
	rows[0].SecretHash[0] = 99
	rows[0].SecretEnc = []byte("caller-owned")
	*rows[0].ExpiresAt = now.Add(24 * time.Hour)

	dst := NewService(dstStore)
	got, err := dst.Get(secret.ID)
	if err != nil {
		t.Fatalf("Get restored credential: %v", err)
	}
	if got.ID != secret.ID || got.SAID != "sa_app" || got.Protocol != ProtocolNFS ||
		got.Resource != "volume/devdisk" || got.Mode != ModeRW || got.SecretHint == "" ||
		!got.CreatedAt.Equal(now) || got.CreatedBy != "admin" {
		t.Fatalf("restored credential mismatch: %+v", got)
	}
	if got.ExpiresAt == nil || !got.ExpiresAt.Equal(exp) {
		t.Fatalf("restored ExpiresAt = %v, want %s", got.ExpiresAt, exp)
	}
	if got.RevokedAt == nil || !got.RevokedAt.Equal(revoked) {
		t.Fatalf("restored RevokedAt = %v, want %s", got.RevokedAt, revoked)
	}
	if got.LastUsedAt == nil || !got.LastUsedAt.Equal(used) {
		t.Fatalf("restored LastUsedAt = %v, want %s", got.LastUsedAt, used)
	}
	if got.SecretHash[0] == 99 {
		t.Fatal("Restore retained caller-owned SecretHash backing data")
	}
	if got.SecretHint == secret.Secret {
		t.Fatal("Get exposed plaintext secret after restore")
	}

	list := dst.List(ListFilter{})
	if len(list) != 1 {
		t.Fatalf("List len = %d, want 1", len(list))
	}
	if list[0].SecretHint == secret.Secret {
		t.Fatal("List exposed plaintext secret after restore")
	}
}
