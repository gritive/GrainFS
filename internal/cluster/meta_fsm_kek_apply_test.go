package cluster

import (
	"bytes"
	"errors"
	"fmt"
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

// TestFSM_Apply_KEKRotate_ContentMismatch_PoisonsFSM verifies the fatal-halt
// discipline. Pre-place a different K1 (same version, wrong bytes) in the
// KEKStore so applyKEKRotate hits the in-memory content-mismatch path and
// returns a wrapped ErrFSMKEKFatal. The apply loop (simulated here via
// MarkFatalHalted) must poison the FSM, and Snapshot must refuse afterwards.
func TestFSM_Apply_KEKRotate_ContentMismatch_PoisonsFSM(t *testing.T) {
	fx := newKEKRotateTestFixture(t)
	requestID := [16]byte{0xBB}
	cmd := fx.buildHappyCmd(requestID)

	// Pre-place a *different* K1 so the duplicate-add branch finds a mismatch.
	differentK1 := bytes.Repeat([]byte{0xDE}, encrypt.KEKSize)
	if err := fx.fsm.KEKStore().Add(cmd.NewVersion, differentK1); err != nil {
		t.Fatalf("pre-place different K1: %v", err)
	}

	envelope := fx.encodeAndWrap(cmd)
	err := fx.fsm.applyCmdAtIndex(envelope, 42)
	if err == nil {
		t.Fatalf("expected fatal error, got nil")
	}
	if !errors.Is(err, ErrFSMKEKFatal) {
		t.Errorf("expected ErrFSMKEKFatal, got: %v", err)
	}

	// Simulate what runApplyLoop does on ErrFSMKEKFatal: poison the FSM.
	fx.fsm.MarkFatalHalted(err)

	// FSM must now be poisoned.
	if haltErr := fx.fsm.FatalHaltedErr(); haltErr == nil {
		t.Fatalf("FSM not poisoned after MarkFatalHalted")
	}

	// Snapshot must refuse.
	if _, snapErr := fx.fsm.Snapshot(); snapErr == nil {
		t.Fatalf("Snapshot must refuse after fatal halt")
	}

	// Subsequent applyCmdAtIndex must short-circuit without dispatching.
	happyCmd := fx.buildHappyCmd([16]byte{0xCC})
	happyEnvelope := fx.encodeAndWrap(happyCmd)
	err2 := fx.fsm.applyCmdAtIndex(happyEnvelope, 43)
	if err2 == nil {
		t.Fatalf("expected halted error on second Apply, got nil")
	}
	// Active version must not have advanced during the second Apply.
	if got := fx.fsm.ActiveKEKVersion(); got != 0 {
		t.Errorf("active_kek_version mutated after halt: got %d, want 0", got)
	}
}

// TestFSM_Apply_KEKRotate_StaleNoOp_DoesNotPoisonFSM verifies that a
// wrap_set_hash mismatch (deterministic stale no-op) does not halt the FSM
// and records RotationStatusStaleNoOp.
func TestFSM_Apply_KEKRotate_StaleNoOp_DoesNotPoisonFSM(t *testing.T) {
	fx := newKEKRotateTestFixture(t)
	cmd := fx.buildHappyCmd([16]byte{0x88})
	// Intentionally wrong hash triggers the stale_noop path.
	cmd.WrapSetHash = bytes.Repeat([]byte{0xFF}, 32)
	envelope := fx.encodeAndWrap(cmd)

	if err := fx.fsm.applyCmdAtIndex(envelope, 7); err != nil {
		t.Errorf("stale_noop should be non-fatal, got error: %v", err)
	}

	// FSM must not be poisoned.
	if haltErr := fx.fsm.FatalHaltedErr(); haltErr != nil {
		t.Fatalf("FSM unexpectedly halted on stale_noop: %v", haltErr)
	}

	// Status recorded as StaleNoOp.
	s, ok := fx.fsm.LookupRotationRequestStatus(cmd.RequestID)
	if !ok || s != RotationStatusStaleNoOp {
		t.Errorf("status = %v ok=%v, want StaleNoOp", s, ok)
	}

	// Snapshot must still work.
	if _, snapErr := fx.fsm.Snapshot(); snapErr != nil {
		t.Errorf("Snapshot must succeed after stale_noop: %v", snapErr)
	}
}

// --- KEKRetire FSM Apply tests ---

// kekRetireTestFixture builds an FSM with active=5 and versions 3,4,5 loaded.
// Version 3 is the target for retire operations.
type kekRetireTestFixture struct {
	t   *testing.T
	fsm *MetaFSM
}

func newKEKRetireTestFixture(t *testing.T) *kekRetireTestFixture {
	t.Helper()
	f := &kekRetireTestFixture{t: t, fsm: NewMetaFSM()}

	store := encrypt.NewKEKStore()
	for _, v := range []uint32{0, 1, 2, 3, 4} {
		kek := bytes.Repeat([]byte{byte(0xA0 + v)}, encrypt.KEKSize)
		if err := store.Add(v, kek); err != nil {
			t.Fatalf("seed KEKStore v=%d: %v", v, err)
		}
	}
	if err := store.SetActiveVersion(4); err != nil {
		t.Fatalf("set active 4: %v", err)
	}
	f.fsm.SetKEKStore(store)
	f.fsm.SetActiveKEKVersion(4)

	// Wire a minimal DEKKeeper so Snapshot works.
	k0 := bytes.Repeat([]byte{0xA0}, encrypt.KEKSize)
	plainDEK := bytes.Repeat([]byte{0xD1}, encrypt.DEKSize)
	wrappedDEK, err := encrypt.AESGCMSeal(k0, plainDEK)
	if err != nil {
		t.Fatalf("seal DEK: %v", err)
	}
	keeper, err := encrypt.LoadFromFSM(k0, map[uint32][]byte{1: wrappedDEK})
	if err != nil {
		t.Fatalf("LoadFromFSM: %v", err)
	}
	f.fsm.SetDEKKeeper(keeper)

	return f
}

func (f *kekRetireTestFixture) buildRetireCmd(version uint32, requestID [16]byte) KEKRetireCmd {
	return KEKRetireCmd{
		PayloadVersion:        1,
		Version:               version,
		Confirm:               fmt.Sprintf("delete-permanently-%d", version),
		Actor:                 "admin@uds",
		RequestID:             requestID,
		RequestedAtUnixNanos:  1717000000000000000,
		ClusterStateAtPropose: ClusterStateAtPropose{ActiveKEKVersion: 4, RetainedKEKCount: 5, LiveDEKGenCount: 1},
	}
}

func (f *kekRetireTestFixture) encodeAndWrap(cmd KEKRetireCmd) []byte {
	f.t.Helper()
	payload, err := EncodeMetaKEKRetireCmd(cmd)
	if err != nil {
		f.t.Fatalf("encode MetaKEKRetireCmd: %v", err)
	}
	envelope, err := encodeMetaCmd(MetaCmdTypeKEKRetire, payload)
	if err != nil {
		f.t.Fatalf("encode MetaCmd envelope: %v", err)
	}
	return envelope
}

// TestFSM_Apply_KEKRetire_HappyPath verifies that a valid retire command marks
// version 3 as Retiring, records the retire commit index, and does NOT remove
// the key from the keystore (that is Prune's responsibility).
func TestFSM_Apply_KEKRetire_HappyPath(t *testing.T) {
	fx := newKEKRetireTestFixture(t)
	requestID := [16]byte{0x44}
	cmd := fx.buildRetireCmd(3, requestID)
	envelope := fx.encodeAndWrap(cmd)

	if err := fx.fsm.applyCmdAtIndex(envelope, 100); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	_, status, retireIdx, ok := fx.fsm.LookupKEKStatus(3)
	if !ok || status != KEKLifecycleRetiring {
		t.Errorf("kek_status[3] = %v ok=%v, want Retiring", status, ok)
	}
	if retireIdx == 0 {
		t.Errorf("retire_commit_index not set (got 0)")
	}

	// Key must still be present — Prune removes it, not Retire.
	if !fx.fsm.KEKStore().HasVersion(3) {
		t.Errorf("KEK version 3 prematurely removed during Retire")
	}

	// keystore.IsRetiring must report true.
	if !fx.fsm.KEKStore().IsRetiring(3) {
		t.Errorf("KEKStore.IsRetiring(3) = false, want true")
	}
}

// TestFSM_Apply_KEKRetire_RejectsActiveVersion verifies that retiring the
// active version returns a non-fatal error.
func TestFSM_Apply_KEKRetire_RejectsActiveVersion(t *testing.T) {
	fx := newKEKRetireTestFixture(t)
	cmd := KEKRetireCmd{
		PayloadVersion: 1,
		Version:        4, // active version
		Confirm:        "delete-permanently-4",
		RequestID:      [16]byte{0x01},
	}
	envelope := fx.encodeAndWrap(cmd)

	err := fx.fsm.applyCmdAtIndex(envelope, 1)
	if err == nil {
		t.Fatalf("expected error for retiring active version, got nil")
	}
	if errors.Is(err, ErrFSMKEKFatal) {
		t.Errorf("retiring active version must NOT be fatal: %v", err)
	}
}

// TestFSM_Apply_KEKRetire_RejectsBadConfirm verifies that a wrong confirm token
// returns a non-fatal error before any state change.
func TestFSM_Apply_KEKRetire_RejectsBadConfirm(t *testing.T) {
	fx := newKEKRetireTestFixture(t)
	cmd := KEKRetireCmd{
		PayloadVersion: 1,
		Version:        3,
		Confirm:        "delete-permanently-2", // wrong version number in token
		RequestID:      [16]byte{0x02},
	}
	envelope := fx.encodeAndWrap(cmd)

	err := fx.fsm.applyCmdAtIndex(envelope, 1)
	if err == nil {
		t.Fatalf("expected confirm mismatch error, got nil")
	}
	if errors.Is(err, ErrFSMKEKFatal) {
		t.Errorf("bad confirm must NOT be fatal: %v", err)
	}
}

// TestFSM_Apply_KEKRetire_IdempotentReplay verifies that applying the same
// retire command twice is a no-op on the second apply.
func TestFSM_Apply_KEKRetire_IdempotentReplay(t *testing.T) {
	fx := newKEKRetireTestFixture(t)
	requestID := [16]byte{0x55}
	cmd := fx.buildRetireCmd(3, requestID)
	envelope := fx.encodeAndWrap(cmd)

	// First apply.
	if err := fx.fsm.applyCmdAtIndex(envelope, 50); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	_, status1, idx1, _ := fx.fsm.LookupKEKStatus(3)

	// Second apply (snapshot tail replay).
	if err := fx.fsm.applyCmdAtIndex(envelope, 50); err != nil {
		t.Errorf("idempotent replay failed: %v", err)
	}
	_, status2, idx2, _ := fx.fsm.LookupKEKStatus(3)
	if status2 != status1 {
		t.Errorf("status changed on replay: %v → %v", status1, status2)
	}
	if idx2 != idx1 {
		t.Errorf("retire_commit_index changed on replay: %d → %d", idx1, idx2)
	}
}

// TestFSM_Apply_KEKRetire_MissingKEKIsFatal verifies that a retire command for
// a version not in the keystore (but < active) returns ErrFSMKEKFatal,
// indicating node-local divergence that must halt the apply loop.
func TestFSM_Apply_KEKRetire_MissingKEKIsFatal(t *testing.T) {
	fx := newKEKRetireTestFixture(t)

	// Remove version 2 from the keystore to simulate local divergence.
	// The fixture loads versions 0-4 with active=4. Delete version 2 so it
	// is legitimately < active but missing from this node's keystore.
	if err := fx.fsm.KEKStore().Delete(2); err != nil {
		t.Fatalf("setup: delete v2: %v", err)
	}

	cmd := KEKRetireCmd{
		PayloadVersion:        1,
		Version:               2, // < active(4), but not in keystore
		Confirm:               "delete-permanently-2",
		RequestID:             [16]byte{0x66},
		ClusterStateAtPropose: ClusterStateAtPropose{ActiveKEKVersion: 4},
	}
	envelope := fx.encodeAndWrap(cmd)

	err := fx.fsm.applyCmdAtIndex(envelope, 1)
	if err == nil {
		t.Fatalf("expected fatal error, got nil")
	}
	if !errors.Is(err, ErrFSMKEKFatal) {
		t.Errorf("expected ErrFSMKEKFatal, got: %v", err)
	}
}
