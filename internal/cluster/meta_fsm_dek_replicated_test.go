package cluster

import (
	"crypto/rand"
	"math"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// newTestDEKKEK returns a fresh random 32-byte KEK for DEK-replicated apply tests.
func newTestDEKKEK(t *testing.T) []byte {
	t.Helper()
	kek := make([]byte, encrypt.KEKSize)
	if _, err := rand.Read(kek); err != nil {
		t.Fatalf("rand kek: %v", err)
	}
	return kek
}

func TestApplyDEKReplicatedRotate_FollowerInstallsLeaderBytes(t *testing.T) {
	kek := newTestDEKKEK(t)
	cid := dekTestClusterID()

	// leader keeper with gen-0
	leaderKeeper, err := encrypt.NewDEKKeeper(kek, cid)
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	lv, _ := leaderKeeper.VersionsAndActive()

	// follower FSM with an EMPTY keeper (gen-0 not yet installed)
	fk, err := encrypt.NewEmptyDEKKeeper(kek, cid)
	if err != nil {
		t.Fatalf("NewEmptyDEKKeeper: %v", err)
	}
	fsm := newTestMetaFSMWithDEKKeeper(t, fk)

	// gen-0 bootstrap: ExpectedActiveGen = MaxUint32 sentinel
	cmd := DEKReplicatedRotateCmd{Gen: 0, WrappedDEK: lv[0], ExpectedActiveGen: math.MaxUint32, ActiveKEKVer: 0}
	enc, err := EncodeDEKReplicatedRotateCmd(cmd)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	if err := fsm.applyDEKReplicatedRotate(10, enc); err != nil {
		t.Fatalf("apply: %v", err)
	}
	fv, active := fk.VersionsAndActive()
	if active != 0 || string(fv[0]) != string(lv[0]) {
		t.Fatalf("follower did not install leader gen-0 (active=%d)", active)
	}
	// idempotent replay at a later index
	if err := fsm.applyDEKReplicatedRotate(11, enc); err != nil {
		t.Fatalf("replay: %v", err)
	}
}

// TestApplyDEKReplicatedRotate_ReplayUnderHistoricalKEK reproduces the
// leader-rejoin / node-restart self-shutdown bug: after a committed KEK rotate
// (v0 -> v1), a restarting node boots its DEKKeeper with the ACTIVE KEK (v1),
// then the raft log replays the gen-0 DEK bootstrap entry whose bytes were
// sealed under KEK v0 (ActiveKEKVer=0). Unwrapping that entry under v1 fails
// AES-GCM auth -> fatal KEK apply -> the node halts and never finishes boot.
//
// The Apply must unwrap each DEKReplicatedRotate under the KEK version named in
// the command (cmd.ActiveKEKVer), resolved from the keystore — not under the
// keeper's current active KEK.
func TestApplyDEKReplicatedRotate_ReplayUnderHistoricalKEK(t *testing.T) {
	cid := dekTestClusterID()
	kek0 := newTestDEKKEK(t)
	kek1 := newTestDEKKEK(t)

	// Leader sealed the gen-0 bootstrap DEK under KEK v0 (kekVer=0).
	leaderKeeper, err := encrypt.NewDEKKeeper(kek0, cid)
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	lv, _ := leaderKeeper.VersionsAndActive()

	// Restarting node: keeper booted with the ACTIVE KEK = v1, plus a keystore
	// holding BOTH v0 and v1 (active=1). This mirrors wireDEKKeeper after a
	// committed v0->v1 KEK rotation.
	restartKeeper, err := encrypt.NewEmptyDEKKeeper(kek1, cid)
	if err != nil {
		t.Fatalf("NewEmptyDEKKeeper: %v", err)
	}
	// wireDEKKeeper labels the keeper with the keystore's active KEK version.
	restartKeeper.SetActiveKEKVersion(1)
	fsm := newTestMetaFSMWithDEKKeeper(t, restartKeeper)
	store := encrypt.NewKEKStore()
	if err := store.Add(0, kek0); err != nil {
		t.Fatalf("store add v0: %v", err)
	}
	if err := store.Add(1, kek1); err != nil {
		t.Fatalf("store add v1: %v", err)
	}
	if err := store.SetActiveVersion(1); err != nil {
		t.Fatalf("store set active 1: %v", err)
	}
	fsm.SetKEKStore(store)

	// Replay the gen-0 bootstrap entry as committed at bootstrap: sealed under
	// KEK v0, ActiveKEKVer=0.
	cmd := DEKReplicatedRotateCmd{Gen: 0, WrappedDEK: lv[0], ExpectedActiveGen: math.MaxUint32, ActiveKEKVer: 0}
	enc, err := EncodeDEKReplicatedRotateCmd(cmd)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := fsm.applyDEKReplicatedRotate(1, enc); err != nil {
		t.Fatalf("replay gen-0 bootstrap under historical KEK v0 must succeed, got: %v", err)
	}
	fv, active := restartKeeper.VersionsAndActive()
	if active != 0 || string(fv[0]) != string(lv[0]) {
		t.Fatalf("gen-0 not installed after replay (active=%d, has=%v)", active, fv[0] != nil)
	}
}

func TestApplyDEKReplicatedRotate_StaleExpectedGenIsNoOp(t *testing.T) {
	kek := newTestDEKKEK(t)
	cid := dekTestClusterID()
	k, err := encrypt.NewDEKKeeper(kek, cid) // active gen 0, kekVer 0
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	fsm := newTestMetaFSMWithDEKKeeper(t, k)

	// leader observed activeGen=5 (stale: real active is 0). Must be a
	// deterministic no-op, NOT fatal, NOT an install.
	leaderK, err := encrypt.NewDEKKeeper(kek, cid)
	if err != nil {
		t.Fatalf("NewDEKKeeper leader: %v", err)
	}
	w, ver, err := leaderK.GenerateWrappedDEK(6)
	if err != nil {
		t.Fatalf("GenerateWrappedDEK: %v", err)
	}
	cmd := DEKReplicatedRotateCmd{Gen: 6, WrappedDEK: w, ExpectedActiveGen: 5, ActiveKEKVer: ver}
	enc, err := EncodeDEKReplicatedRotateCmd(cmd)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := fsm.applyDEKReplicatedRotate(12, enc); err != nil {
		t.Fatalf("stale cmd must be no-op, got err: %v", err)
	}
	if _, a := k.VersionsAndActive(); a != 0 {
		t.Fatalf("stale cmd must not advance active; got %d", a)
	}
}

func TestApplyDEKReplicatedRotate_BootstrapSentinelRejectedWhenGensExist(t *testing.T) {
	kek := newTestDEKKEK(t)
	cid := dekTestClusterID()
	k, err := encrypt.NewDEKKeeper(kek, cid) // already has gen-0
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	fsm := newTestMetaFSMWithDEKKeeper(t, k)

	leaderK, err := encrypt.NewDEKKeeper(kek, cid)
	if err != nil {
		t.Fatalf("NewDEKKeeper leader: %v", err)
	}
	lv, _ := leaderK.VersionsAndActive()
	// sentinel bootstrap cmd arriving at a keeper that ALREADY has gens →
	// deterministic no-op (defense-in-depth against an abused ungated path).
	cmd := DEKReplicatedRotateCmd{Gen: 0, WrappedDEK: lv[0], ExpectedActiveGen: math.MaxUint32, ActiveKEKVer: 0}
	enc, err := EncodeDEKReplicatedRotateCmd(cmd)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := fsm.applyDEKReplicatedRotate(13, enc); err != nil {
		t.Fatalf("sentinel-over-existing must be no-op, got err: %v", err)
	}
}

func TestApplyDEKReplicatedRotate_BootstrapSentinelNonZeroGenIsNoOp(t *testing.T) {
	// MEDIUM 1 / Pass 2: a malformed ungated sentinel command with gen != 0 must
	// NOT install (it would otherwise plant a bogus genesis gen on an empty keeper).
	kek := newTestDEKKEK(t)
	cid := dekTestClusterID()
	k, err := encrypt.NewEmptyDEKKeeper(kek, cid) // empty: no gens yet
	if err != nil {
		t.Fatalf("NewEmptyDEKKeeper: %v", err)
	}
	fsm := newTestMetaFSMWithDEKKeeper(t, k)

	leaderK, err := encrypt.NewDEKKeeper(kek, cid)
	if err != nil {
		t.Fatalf("NewDEKKeeper leader: %v", err)
	}
	w, ver, err := leaderK.GenerateWrappedDEK(5)
	if err != nil {
		t.Fatalf("GenerateWrappedDEK: %v", err)
	}
	cmd := DEKReplicatedRotateCmd{Gen: 5, WrappedDEK: w, ExpectedActiveGen: math.MaxUint32, ActiveKEKVer: ver}
	enc, err := EncodeDEKReplicatedRotateCmd(cmd)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := fsm.applyDEKReplicatedRotate(14, enc); err != nil {
		t.Fatalf("sentinel with gen!=0 must be no-op, got err: %v", err)
	}
	if v, _ := k.VersionsAndActive(); len(v) != 0 {
		t.Fatalf("sentinel with gen!=0 must NOT install; keeper has %d gens", len(v))
	}
}
