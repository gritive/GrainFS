package cluster

import (
	"bytes"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/iam/iampb"
)

// newIAMTestEncryptor builds a deterministic 32-byte AES-256-GCM key
// suitable for IAM snapshot round-trip tests.
func newIAMTestEncryptor(t *testing.T) *encrypt.Encryptor {
	t.Helper()
	key := bytes.Repeat([]byte{0xab}, 32)
	enc, err := encrypt.NewEncryptor(key)
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}
	return enc
}

// buildSACreatePayloadForTest mirrors iam.buildSACreatePayload (unexported).
func buildSACreatePayloadForTest(saID, name string, ts time.Time) []byte {
	b := flatbuffers.NewBuilder(64)
	idOff := b.CreateString(saID)
	nameOff := b.CreateString(name)
	descOff := b.CreateString("")
	cbOff := b.CreateString("")
	iampb.SACreatePayloadStart(b)
	iampb.SACreatePayloadAddSaId(b, idOff)
	iampb.SACreatePayloadAddName(b, nameOff)
	iampb.SACreatePayloadAddDescription(b, descOff)
	iampb.SACreatePayloadAddCreatedAtUnixNs(b, ts.UnixNano())
	iampb.SACreatePayloadAddCreatedBy(b, cbOff)
	b.Finish(iampb.SACreatePayloadEnd(b))
	return b.FinishedBytes()
}

func buildKeyCreatePayloadForTest(ak, saID string, encBytes []byte, ts time.Time) []byte {
	b := flatbuffers.NewBuilder(128)
	akOff := b.CreateString(ak)
	saOff := b.CreateString(saID)
	encOff := b.CreateByteVector(encBytes)
	iampb.KeyCreatePayloadStart(b)
	iampb.KeyCreatePayloadAddAccessKey(b, akOff)
	iampb.KeyCreatePayloadAddSecretKeyEnc(b, encOff)
	iampb.KeyCreatePayloadAddSaId(b, saOff)
	iampb.KeyCreatePayloadAddCreatedAtUnixNs(b, ts.UnixNano())
	b.Finish(iampb.KeyCreatePayloadEnd(b))
	return b.FinishedBytes()
}

func buildGrantWildcardPutPayloadForTest(saID string, role iam.Role, ts time.Time) []byte {
	b := flatbuffers.NewBuilder(64)
	saOff := b.CreateString(saID)
	cbOff := b.CreateString("")
	iampb.GrantWildcardPutPayloadStart(b)
	iampb.GrantWildcardPutPayloadAddSaId(b, saOff)
	iampb.GrantWildcardPutPayloadAddRole(b, iampb.Role(role))
	iampb.GrantWildcardPutPayloadAddCreatedAtUnixNs(b, ts.UnixNano())
	iampb.GrantWildcardPutPayloadAddCreatedBy(b, cbOff)
	b.Finish(iampb.GrantWildcardPutPayloadEnd(b))
	return b.FinishedBytes()
}

// TestMetaFSM_Snapshot_IncludesIAMState round-trips an IAM-populated FSM
// through Snapshot+Restore on a fresh FSM and verifies that SAs, keys,
// wildcard grants, and the sticky auth_enabled bit all survive.
//
// Pre-fix: MetaFSM.Snapshot serialized only 8 in-memory fields and dropped
// the IAM substore entirely. Raft log compaction (default 30s
// LogGCInterval) then truncated the IAM raft entries → restart restored a
// permissive cluster despite operators having set up SAs/grants.
func TestMetaFSM_Snapshot_IncludesIAMState(t *testing.T) {
	enc := newIAMTestEncryptor(t)
	store := iam.NewStore()
	applier := iam.NewApplier(store, enc)

	// Seed IAM via the apply path (mirrors raft commit).
	now := time.Unix(1700000000, 0).UTC()
	if err := applier.ApplySACreate(buildSACreatePayloadForTest("sa-test", "test", now)); err != nil {
		t.Fatalf("ApplySACreate: %v", err)
	}
	wrapped, err := iam.WrapSecret(enc, "sa-test", "the-secret-xyz")
	if err != nil {
		t.Fatalf("WrapSecret: %v", err)
	}
	if err := applier.ApplyKeyCreate(buildKeyCreatePayloadForTest("AKTEST123", "sa-test", wrapped, now)); err != nil {
		t.Fatalf("ApplyKeyCreate: %v", err)
	}
	if err := applier.ApplyGrantWildcardPut(buildGrantWildcardPutPayloadForTest("sa-test", iam.RoleAdmin, now)); err != nil {
		t.Fatalf("ApplyGrantWildcardPut: %v", err)
	}
	if err := applier.ApplyAuthEnable(nil); err != nil {
		t.Fatalf("ApplyAuthEnable: %v", err)
	}

	f := NewMetaFSM()
	f.SetIAM(store, applier)

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Restore into a fresh FSM with a fresh Store but the same encryptor.
	store2 := iam.NewStore()
	applier2 := iam.NewApplier(store2, enc)
	f2 := NewMetaFSM()
	f2.SetIAM(store2, applier2)
	if err := f2.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if _, ok := store2.LookupSA("sa-test"); !ok {
		t.Fatal("SA lost across snapshot round-trip")
	}
	if !store2.AuthEnabled() {
		t.Fatal("auth_enabled bit lost across snapshot round-trip")
	}
	if got := store2.LookupGrant("sa-test", "any-bucket"); got != iam.RoleAdmin {
		t.Fatalf("wildcard grant lost: got %v, want RoleAdmin", got)
	}
	k, ok := store2.LookupKey("AKTEST123")
	if !ok {
		t.Fatal("AccessKey lost across snapshot round-trip")
	}
	if k.SecretKey != "the-secret-xyz" {
		t.Fatalf("decrypted secret = %q, want the-secret-xyz", k.SecretKey)
	}
}

// TestMetaFSM_Snapshot_NoIAMData_BackwardCompat verifies that a snapshot
// taken from an FSM whose iamStore is empty still round-trips cleanly,
// and does not flip auth_enabled or invent any SAs on restore.
func TestMetaFSM_Snapshot_NoIAMData_BackwardCompat(t *testing.T) {
	enc := newIAMTestEncryptor(t)
	f := NewMetaFSM()
	f.SetIAM(iam.NewStore(), iam.NewApplier(iam.NewStore(), enc))

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot empty IAM: %v", err)
	}

	store2 := iam.NewStore()
	applier2 := iam.NewApplier(store2, enc)
	f2 := NewMetaFSM()
	f2.SetIAM(store2, applier2)
	if err := f2.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if !store2.IsEmpty() {
		t.Fatal("empty IAM snapshot caused dst to gain SAs")
	}
	if store2.AuthEnabled() {
		t.Fatal("empty IAM snapshot flipped auth_enabled")
	}
}

// TestMetaFSM_Snapshot_LegacySnapshot_Restores_NoIAM confirms that snapshots
// produced by pre-Phase-5d code (no IAM trailer) still restore cleanly. We
// fabricate a legacy snapshot by stripping the trailer from a fresh one.
func TestMetaFSM_Snapshot_LegacySnapshot_Restores_NoIAM(t *testing.T) {
	enc := newIAMTestEncryptor(t)
	f := NewMetaFSM()
	f.SetIAM(iam.NewStore(), iam.NewApplier(iam.NewStore(), enc))

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	// Strip [u32 iam_len][u32 magic] trailer (and the 0-length IAM section,
	// which is itself zero bytes for an empty store).
	if len(snap) < iamSnapshotTrailerLen {
		t.Fatalf("snapshot too small to contain trailer: %d", len(snap))
	}
	legacy := snap[:len(snap)-iamSnapshotTrailerLen]

	store2 := iam.NewStore()
	applier2 := iam.NewApplier(store2, enc)
	f2 := NewMetaFSM()
	f2.SetIAM(store2, applier2)
	if err := f2.Restore(legacy); err != nil {
		t.Fatalf("Restore legacy: %v", err)
	}
	if !store2.IsEmpty() {
		t.Fatal("legacy snapshot somehow injected SAs")
	}
	if store2.AuthEnabled() {
		t.Fatal("legacy snapshot somehow flipped auth_enabled")
	}
}
