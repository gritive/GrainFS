package cluster

import (
	"bytes"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/iam/iampb"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
)

// newIAMRawEncryptor returns a *encrypt.Encryptor (legacy raw form) for FSM
// state that still consumes one (alert webhook etc.). IAM-specific paths use
// newIAMTestEncryptor (returns storage.DataEncryptor) after R2.
func newIAMRawEncryptor(t *testing.T) *encrypt.Encryptor {
	t.Helper()
	key := bytes.Repeat([]byte{0xab}, 32)
	enc, err := encrypt.NewEncryptor(key)
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}
	return enc
}

// newIAMTestEncryptor returns a storage.DataEncryptor suitable for IAM
// snapshot round-trip tests. Wraps a deterministic *encrypt.Encryptor in the
// static EncryptorAdapter (gen always 0).
func newIAMTestEncryptor(t *testing.T) storage.DataEncryptor {
	t.Helper()
	clusterID := bytes.Repeat([]byte{0xcd}, 16)
	return storage.NewEncryptorAdapter(newIAMRawEncryptor(t), clusterID)
}

// wireTestKEKAndDEK wires both a KEKStore (K0 active) and a DEKKeeper (gen 0
// active) into fsm, then returns a DataEncryptor backed by the DEKKeeper.
// Use this for IAM-on-DEK tests so the snapshot embeds a DEK trailer that
// Restore's two-pass decode can use (R2). The clusterID is the standard
// byte(i+1) used by wireTestKEK so independent FSMs are interchangeable.
func wireTestKEKAndDEK(t *testing.T, fsm *MetaFSM) storage.DataEncryptor {
	t.Helper()
	wireTestKEK(t, fsm) // installs KEKStore + clusterID
	var clusterID [16]byte
	for i := range clusterID {
		clusterID[i] = byte(i + 1)
	}
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0xA0}, encrypt.KEKSize), clusterID[:])
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	fsm.SetDEKKeeper(keeper)
	return storage.NewDEKKeeperAdapter(keeper, clusterID[:])
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
	return buildKeyCreatePayloadForTestWithGen(ak, saID, encBytes, 0, ts)
}

func buildKeyCreatePayloadForTestWithGen(ak, saID string, encBytes []byte, gen uint32, ts time.Time) []byte {
	b := flatbuffers.NewBuilder(128)
	akOff := b.CreateString(ak)
	saOff := b.CreateString(saID)
	encOff := b.CreateByteVector(encBytes)
	iampb.KeyCreatePayloadStart(b)
	iampb.KeyCreatePayloadAddAccessKey(b, akOff)
	iampb.KeyCreatePayloadAddSecretKeyEnc(b, encOff)
	iampb.KeyCreatePayloadAddSaId(b, saOff)
	iampb.KeyCreatePayloadAddCreatedAtUnixNs(b, ts.UnixNano())
	iampb.KeyCreatePayloadAddSecretKeyDekGen(b, gen)
	b.Finish(iampb.KeyCreatePayloadEnd(b))
	return b.FinishedBytes()
}

// TestMetaFSM_Snapshot_IncludesIAMState round-trips an IAM-populated FSM
// through Snapshot+Restore on a fresh FSM and verifies that SAs and keys
// survive. (WildcardGrant snapshot coverage removed in §2: Role/Grant model gone.)
//
// Pre-fix: MetaFSM.Snapshot serialized only 8 in-memory fields and dropped
// the IAM substore entirely. Raft log compaction (default 30s
// LogGCInterval) then truncated the IAM raft entries → restart restored a
// permissive cluster despite operators having set up SAs/keys.
func TestMetaFSM_Snapshot_IncludesIAMState(t *testing.T) {
	store := iam.NewStore()
	applier := iam.NewApplier(store, nil)

	f := NewMetaFSM()
	enc := wireTestKEKAndDEK(t, f)
	applier.SetEncryptor(enc)
	f.SetIAM(store, applier)

	// Seed IAM via the apply path (mirrors raft commit).
	now := time.Unix(1700000000, 0).UTC()
	if err := applier.ApplySACreate(buildSACreatePayloadForTest("sa-test", "test", now)); err != nil {
		t.Fatalf("ApplySACreate: %v", err)
	}
	wrapped, gen, err := iam.WrapSecret(enc, "sa-test", "AKTEST123", "the-secret-xyz")
	if err != nil {
		t.Fatalf("WrapSecret: %v", err)
	}
	if err := applier.ApplyKeyCreate(buildKeyCreatePayloadForTestWithGen("AKTEST123", "sa-test", wrapped, gen, now)); err != nil {
		t.Fatalf("ApplyKeyCreate: %v", err)
	}

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Restore into a fresh FSM with a fresh Store; applier on the destination
	// has NO encryptor yet (mimics boot order: keeper rebuilt during
	// Restore via the transient adapter, then SetEncryptor wires the live one).
	store2 := iam.NewStore()
	applier2 := iam.NewApplier(store2, nil)
	f2 := NewMetaFSM()
	wireTestKEK(t, f2)
	f2.SetIAM(store2, applier2)
	if err := f2.Restore(raft.SnapshotMeta{}, snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if _, ok := store2.LookupSA("sa-test"); !ok {
		t.Fatal("SA lost across snapshot round-trip")
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
	f := NewMetaFSM()
	enc := wireTestKEKAndDEK(t, f)
	applier := iam.NewApplier(iam.NewStore(), enc)
	f.SetIAM(iam.NewStore(), applier)

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot empty IAM: %v", err)
	}

	store2 := iam.NewStore()
	applier2 := iam.NewApplier(store2, nil)
	f2 := NewMetaFSM()
	wireTestKEK(t, f2)
	f2.SetIAM(store2, applier2)
	if err := f2.Restore(raft.SnapshotMeta{}, snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if !store2.IsEmpty() {
		t.Fatal("empty IAM snapshot caused dst to gain SAs")
	}
}

// TestMetaFSM_Restore_IAM_AtomicCommit verifies the F17 fix: the IAM state
// is committed via RestoreFrom (single atomic state-pointer swap) rather
// than a second ReadSnapshot call that could fail after core FSM fields are
// already committed. Confirms that (a) the SA and key survive the roundtrip,
// and (b) the destination store's LookupKey works immediately after Restore
// (regression guard: the old Reset+ReadSnapshot path was functionally
// equivalent but could error post-commit; RestoreFrom is error-free).
func TestMetaFSM_Restore_IAM_AtomicCommit(t *testing.T) {
	store := iam.NewStore()
	applier := iam.NewApplier(store, nil)

	f := NewMetaFSM()
	enc := wireTestKEKAndDEK(t, f)
	applier.SetEncryptor(enc)
	f.SetIAM(store, applier)

	now := time.Unix(1700000001, 0).UTC()
	if err := applier.ApplySACreate(buildSACreatePayloadForTest("sa-atomic", "atomic-test", now)); err != nil {
		t.Fatalf("ApplySACreate: %v", err)
	}
	wrapped, gen, err := iam.WrapSecret(enc, "sa-atomic", "AKATOMIC1", "super-secret")
	if err != nil {
		t.Fatalf("WrapSecret: %v", err)
	}
	if err := applier.ApplyKeyCreate(buildKeyCreatePayloadForTestWithGen("AKATOMIC1", "sa-atomic", wrapped, gen, now)); err != nil {
		t.Fatalf("ApplyKeyCreate: %v", err)
	}

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Restore into a fresh FSM. RestoreFrom must commit iamTempStore in one
	// atomic pointer swap — no second parse, no error path.
	store2 := iam.NewStore()
	applier2 := iam.NewApplier(store2, nil)
	f2 := NewMetaFSM()
	wireTestKEK(t, f2)
	f2.SetIAM(store2, applier2)
	if err := f2.Restore(raft.SnapshotMeta{}, snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// SA and key must be visible immediately after Restore.
	if _, ok := store2.LookupSA("sa-atomic"); !ok {
		t.Fatal("SA missing after atomic IAM Restore")
	}
	k, ok := store2.LookupKey("AKATOMIC1")
	if !ok {
		t.Fatal("AccessKey missing after atomic IAM Restore")
	}
	if k.SecretKey != "super-secret" {
		t.Fatalf("decrypted secret = %q, want super-secret", k.SecretKey)
	}
}

// TestMetaFSM_Snapshot_LegacySnapshot_Restores_NoIAM confirms that snapshots
// produced by pre-Phase-5d code (no IAM trailer) still restore cleanly. We
// fabricate a legacy snapshot by stripping the trailer from a fresh one.
func TestMetaFSM_Snapshot_LegacySnapshot_Restores_NoIAM(t *testing.T) {
	enc := newIAMTestEncryptor(t)
	f := NewMetaFSM()
	wireTestKEK(t, f)
	f.SetIAM(iam.NewStore(), iam.NewApplier(iam.NewStore(), enc))

	sealed, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	// Decrypt the Phase D-snap envelope to operate on the plaintext trailer.
	snap, err := f.openSnapshotEnvelope(sealed)
	if err != nil {
		t.Fatalf("openSnapshotEnvelope: %v", err)
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
	wireTestKEK(t, f2)
	f2.SetIAM(store2, applier2)
	// Re-seal the stripped plaintext so Restore can decrypt it.
	sealedLegacy, err := f.sealSnapshotEnvelope(legacy)
	if err != nil {
		t.Fatalf("sealSnapshotEnvelope (legacy): %v", err)
	}
	if err := f2.Restore(raft.SnapshotMeta{}, sealedLegacy); err != nil {
		t.Fatalf("Restore legacy: %v", err)
	}
	if !store2.IsEmpty() {
		t.Fatal("legacy snapshot somehow injected SAs")
	}
}
