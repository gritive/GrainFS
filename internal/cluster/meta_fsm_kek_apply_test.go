package cluster

import (
	"bytes"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// kekRotateTestFixture wires the FSM with a KEKStore, DEKKeeper, clusterID,
// and one DEK generation. It is the shared seed for KEK rotation Apply
// tests: active KEK version 0, DEK gen 1 wrapped under K0.
type kekRotateTestFixture struct {
	t                *testing.T
	fsm              *MetaFSM
	clusterID        [16]byte
	k0               []byte
	k1               []byte
	plainDEK         []byte
	wrappedDEKK0     []byte // DEK plaintext sealed under K0 (FSM-resident wrap)
	wrappedDEKK1     []byte // DEK plaintext sealed under K1 (rewrap payload entry)
	wrappedK1UnderK0 []byte // K1 sealed under K0 with rotation AAD
}

func newKEKRotateTestFixture(t *testing.T) *kekRotateTestFixture {
	t.Helper()
	f := &kekRotateTestFixture{
		t:        t,
		fsm:      NewMetaFSM(),
		k0:       bytes.Repeat([]byte{0xA0}, encrypt.KEKSize),
		k1:       bytes.Repeat([]byte{0xA1}, encrypt.KEKSize),
		plainDEK: bytes.Repeat([]byte{0xD1}, encrypt.DEKSize),
	}
	// Cluster ID — arbitrary 16 bytes.
	for i := range f.clusterID {
		f.clusterID[i] = byte(i + 1)
	}
	f.fsm.SetClusterID(f.clusterID[:])

	// Seed KEKStore with K0 as active.
	store := encrypt.NewKEKStore()
	if err := store.Add(0, f.k0); err != nil {
		t.Fatalf("seed KEKStore: %v", err)
	}
	f.fsm.SetKEKStore(store)

	// Seal the canonical DEK plaintext under K0 to produce the FSM-resident
	// wrap. Build a DEKKeeper at gen 1 (matches plan's seed) via LoadFromFSM.
	var err error
	f.wrappedDEKK0, err = encrypt.AESGCMSeal(f.k0, f.plainDEK)
	if err != nil {
		t.Fatalf("seal DEK under K0: %v", err)
	}
	keeper, err := encrypt.LoadFromFSM(f.k0, map[uint32][]byte{1: f.wrappedDEKK0})
	if err != nil {
		t.Fatalf("LoadFromFSM: %v", err)
	}
	f.fsm.SetDEKKeeper(keeper)

	// Rewrap the same plaintext under K1.
	f.wrappedDEKK1, err = encrypt.AESGCMSeal(f.k1, f.plainDEK)
	if err != nil {
		t.Fatalf("seal DEK under K1: %v", err)
	}

	// Seal K1 under K0 with the canonical rotation AAD for NewVersion=1.
	aad := encrypt.BuildAAD(encrypt.DomainKEKRotate, f.clusterID[:], encrypt.FieldUint32(1))
	f.wrappedK1UnderK0, err = encrypt.AESGCMSealWithAAD(f.k0, f.k1, aad)
	if err != nil {
		t.Fatalf("wrap K1 under K0: %v", err)
	}

	return f
}

func (f *kekRotateTestFixture) wrapSetHash() []byte {
	h := encrypt.CanonicalWrapSetHash([]encrypt.WrapSetEntry{{Gen: 1, Wrap: f.wrappedDEKK0}})
	return h[:]
}

func (f *kekRotateTestFixture) buildHappyCmd(requestID [16]byte) KEKRotateCmd {
	return KEKRotateCmd{
		PayloadVersion:        1,
		NewVersion:            1,
		WrappedNewKEK:         f.wrappedK1UnderK0,
		WrapSetHash:           f.wrapSetHash(),
		RewrappedDEKs:         []RewrappedDEKEntry{{Gen: 1, Wrapped: f.wrappedDEKK1}},
		Confirm:               "rotate-now",
		Actor:                 "admin@uds",
		RequestID:             requestID,
		RequestedAtUnixNanos:  1717000000000000000,
		ClusterStateAtPropose: ClusterStateAtPropose{ActiveKEKVersion: 0, RetainedKEKCount: 1, LiveDEKGenCount: 1},
	}
}

func (f *kekRotateTestFixture) encodeAndWrap(cmd KEKRotateCmd) []byte {
	f.t.Helper()
	payload, err := EncodeMetaKEKRotateCmd(cmd)
	if err != nil {
		f.t.Fatalf("encode MetaKEKRotateCmd: %v", err)
	}
	envelope, err := encodeMetaCmd(MetaCmdTypeKEKRotate, payload)
	if err != nil {
		f.t.Fatalf("encode MetaCmd envelope: %v", err)
	}
	return envelope
}

func TestFSM_Apply_KEKRotate_HappyPath_InstallsRewrappedDEKs(t *testing.T) {
	fx := newKEKRotateTestFixture(t)
	requestID := [16]byte{0x77}
	cmd := fx.buildHappyCmd(requestID)
	envelope := fx.encodeAndWrap(cmd)

	if err := fx.fsm.applyCmdAtIndex(envelope, 100); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if got := fx.fsm.ActiveKEKVersion(); got != 1 {
		t.Errorf("active_kek_version = %d, want 1", got)
	}
	if !fx.fsm.KEKStore().HasVersion(1) {
		t.Errorf("keystore missing version 1 after rotate Apply")
	}
	if act := fx.fsm.KEKStore().ActiveVersion(); act != 1 {
		t.Errorf("KEKStore.ActiveVersion = %d, want 1", act)
	}

	// FSM wrap[1] must be byte-identical to the payload — no node-side reseal.
	keeperWraps, _ := fx.fsm.dekKeeper.VersionsAndActive()
	got, ok := keeperWraps[1]
	if !ok {
		t.Fatalf("keeper missing wrap[1] after Apply")
	}
	if !bytes.Equal(got, fx.wrappedDEKK1) {
		t.Errorf("wrap[1] differs from payload bytes — nodes would diverge on re-seal")
	}

	// Request status records the applied outcome.
	status, found := fx.fsm.LookupRotationRequestStatus(requestID)
	if !found || status != RotationStatusApplied {
		t.Errorf("request status = %v found=%v, want Applied", status, found)
	}

	// Sanity: the keeper can still Seal/Open with the new KEK.
	ct, _, err := fx.fsm.dekKeeper.Seal([]byte("post-rotate"))
	if err != nil {
		t.Fatalf("Seal after rotate: %v", err)
	}
	pt, err := fx.fsm.dekKeeper.Open(ct, 1)
	if err != nil {
		t.Fatalf("Open after rotate: %v", err)
	}
	if !bytes.Equal(pt, []byte("post-rotate")) {
		t.Errorf("roundtrip mismatch after rotate")
	}
}

func TestFSM_Apply_KEKRotate_VersionMismatch_Rejects(t *testing.T) {
	fx := newKEKRotateTestFixture(t)
	fx.fsm.SetActiveKEKVersion(5)
	// Re-add KEKStore versions so the active marker matches.
	if err := fx.fsm.KEKStore().Add(5, bytes.Repeat([]byte{0xB5}, encrypt.KEKSize)); err != nil {
		t.Fatalf("seed K5: %v", err)
	}
	if err := fx.fsm.KEKStore().SetActiveVersion(5); err != nil {
		t.Fatalf("SetActive 5: %v", err)
	}

	cmd := KEKRotateCmd{
		PayloadVersion: 1,
		NewVersion:     7, // should be 6
		WrappedNewKEK:  bytes.Repeat([]byte{0xCC}, 60),
		WrapSetHash:    bytes.Repeat([]byte{0}, 32),
		Confirm:        "rotate-now",
		RequestID:      [16]byte{0x10},
	}
	envelope := fx.encodeAndWrap(cmd)
	if err := fx.fsm.applyCmdAtIndex(envelope, 1); err == nil {
		t.Errorf("expected version mismatch error")
	}
}

func TestFSM_Apply_KEKRotate_WrapSetHashMismatch_StaleNoOp(t *testing.T) {
	fx := newKEKRotateTestFixture(t)
	cmd := fx.buildHappyCmd([16]byte{0x88})
	// Intentionally wrong hash.
	cmd.WrapSetHash = bytes.Repeat([]byte{0xFF}, 32)
	envelope := fx.encodeAndWrap(cmd)

	if err := fx.fsm.applyCmdAtIndex(envelope, 7); err != nil {
		t.Errorf("hash mismatch should be deterministic no-op, got error: %v", err)
	}
	status, found := fx.fsm.LookupRotationRequestStatus(cmd.RequestID)
	if !found || status != RotationStatusStaleNoOp {
		t.Errorf("status = %v found=%v, want StaleNoOp", status, found)
	}
	if v := fx.fsm.ActiveKEKVersion(); v != 0 {
		t.Errorf("active_kek_version mutated on stale rotation: got %d, want 0", v)
	}
	if fx.fsm.KEKStore().HasVersion(1) {
		t.Errorf("K1 leaked into KEKStore on stale rotation")
	}
}

func TestFSM_Apply_KEKRotate_ConfirmMismatch_Rejects(t *testing.T) {
	fx := newKEKRotateTestFixture(t)
	cmd := fx.buildHappyCmd([16]byte{0x55})
	cmd.Confirm = "wrong"
	envelope := fx.encodeAndWrap(cmd)
	if err := fx.fsm.applyCmdAtIndex(envelope, 1); err == nil {
		t.Errorf("expected confirm token reject")
	}
}

func TestFSM_Apply_KEKRotate_IdempotentReplay(t *testing.T) {
	fx := newKEKRotateTestFixture(t)
	requestID := [16]byte{0x99}
	cmd := fx.buildHappyCmd(requestID)
	envelope := fx.encodeAndWrap(cmd)

	// First Apply succeeds.
	if err := fx.fsm.applyCmdAtIndex(envelope, 50); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	v1 := fx.fsm.ActiveKEKVersion()

	// Snapshot tail replay — same log entry re-applied.
	if err := fx.fsm.applyCmdAtIndex(envelope, 50); err != nil {
		t.Errorf("idempotent replay failed: %v", err)
	}
	if got := fx.fsm.ActiveKEKVersion(); got != v1 {
		t.Errorf("active_kek_version changed on replay: %d → %d", v1, got)
	}
	// Status still Applied.
	status, found := fx.fsm.LookupRotationRequestStatus(requestID)
	if !found || status != RotationStatusApplied {
		t.Errorf("status after replay = %v found=%v, want Applied", status, found)
	}
	// Keeper still has the payload wrap bytes byte-identically.
	wraps, _ := fx.fsm.dekKeeper.VersionsAndActive()
	if !bytes.Equal(wraps[1], fx.wrappedDEKK1) {
		t.Errorf("keeper wrap[1] diverged on replay")
	}
}
