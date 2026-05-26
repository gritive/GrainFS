package encrypt

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func testClusterID() []byte {
	id := make([]byte, 16)
	for i := range id {
		id[i] = byte(i + 1)
	}
	return id
}

func TestNewEmptyDEKKeeper_SealFailsUntilInstall(t *testing.T) {
	kek := make([]byte, KEKSize)
	rand.Read(kek)
	k, err := NewEmptyDEKKeeper(kek, testClusterID())
	if err != nil {
		t.Fatalf("NewEmptyDEKKeeper: %v", err)
	}
	if _, _, err := k.Seal([]byte("x")); err == nil {
		t.Fatal("Seal must fail on empty keeper (no active DEK)")
	}
}

func TestInstallReplicatedDEK_TwoNodesConverge(t *testing.T) {
	kek := make([]byte, KEKSize)
	rand.Read(kek)
	cid := testClusterID()

	// "leader" generates gen-0 and exports its wrapped bytes.
	leader, _ := NewDEKKeeper(kek, cid)
	versions, active := leader.VersionsAndActive()
	if active != 0 {
		t.Fatalf("active=%d want 0", active)
	}
	wrapped0 := versions[0]

	// "follower" starts empty, installs the SAME wrapped bytes for gen-0
	// under KEK version 0.
	follower, _ := NewEmptyDEKKeeper(kek, cid)
	if err := follower.InstallReplicatedDEK(0, wrapped0, 0); err != nil {
		t.Fatalf("InstallReplicatedDEK: %v", err)
	}

	// Cross-node: ciphertext sealed by leader opens on follower.
	ct, gen, err := leader.Seal([]byte("hello"))
	if err != nil {
		t.Fatalf("leader Seal: %v", err)
	}
	plain, err := follower.Open(ct, gen)
	if err != nil {
		t.Fatalf("follower Open: %v", err)
	}
	if !bytes.Equal(plain, []byte("hello")) {
		t.Fatalf("cross-node decrypt mismatch: %q", plain)
	}

	// Wrap sets are byte-identical -> canonical hashes match.
	fv, _ := follower.VersionsAndActive()
	if !bytes.Equal(fv[0], wrapped0) {
		t.Fatal("follower wrap[0] != leader wrap[0]")
	}
}

func TestGenerateWrappedDEK_InstallableOnPeer(t *testing.T) {
	kek := make([]byte, KEKSize)
	rand.Read(kek)
	cid := testClusterID()

	leader, _ := NewDEKKeeper(kek, cid)                  // active gen 0, kekVer 0
	wrapped, kekVer, err := leader.GenerateWrappedDEK(1) // produce gen-1 bytes; does NOT install locally
	if err != nil {
		t.Fatalf("GenerateWrappedDEK: %v", err)
	}
	if kekVer != 0 {
		t.Fatalf("kekVer=%d want 0", kekVer)
	}
	// leader still at gen 0 (install happens via Apply on every node)
	if _, a := leader.VersionsAndActive(); a != 0 {
		t.Fatalf("GenerateWrappedDEK must NOT mutate active; got %d", a)
	}
	// install on leader and a fresh follower; both converge to gen-1
	if err := leader.InstallReplicatedDEK(1, wrapped, kekVer); err != nil {
		t.Fatalf("leader install: %v", err)
	}
	follower, _ := NewEmptyDEKKeeper(kek, cid)
	// install is per-gen idempotent and independent; install gen-1 directly.
	if err := follower.InstallReplicatedDEK(1, wrapped, kekVer); err != nil {
		t.Fatalf("follower install: %v", err)
	}
	ct, gen, _ := leader.Seal([]byte("v1"))
	got, err := follower.Open(ct, gen)
	if err != nil || string(got) != "v1" {
		t.Fatalf("cross-node gen-1 decrypt: got=%q err=%v", got, err)
	}
}

// A wrap sealed for (gen=2, kekVer=N) must NOT open as (gen=2, kekVer=N+1).
func TestDEKWrapAAD_BoundToGenAndKEKVer(t *testing.T) {
	kek := make([]byte, KEKSize)
	rand.Read(kek)
	cid := testClusterID()
	k, _ := NewDEKKeeper(kek, cid)
	wrapped, kekVer, _ := k.GenerateWrappedDEK(2)
	// correct AAD installs; wrong kekVer fails to unwrap.
	if err := k.InstallReplicatedDEK(2, wrapped, kekVer); err != nil {
		t.Fatalf("install correct AAD: %v", err)
	}
	fresh, _ := NewEmptyDEKKeeper(kek, cid)
	if err := fresh.InstallReplicatedDEK(2, wrapped, kekVer+1); err == nil {
		t.Fatal("install with wrong kekVer must fail AAD auth")
	}
}

// A wrap sealed for one cluster must NOT open under a different clusterID.
func TestDEKWrapAAD_BoundToClusterID(t *testing.T) {
	kek := make([]byte, KEKSize)
	rand.Read(kek)
	cidA := testClusterID()
	cidB := make([]byte, 16)
	for i := range cidB {
		cidB[i] = byte(0xF0 + i)
	}
	leader, _ := NewDEKKeeper(kek, cidA)
	wrapped, kekVer, _ := leader.GenerateWrappedDEK(1)
	other, _ := NewEmptyDEKKeeper(kek, cidB)
	if err := other.InstallReplicatedDEK(1, wrapped, kekVer); err == nil {
		t.Fatal("install under different clusterID must fail AAD auth")
	}
}

// TestGreenfieldGuard_SnapshotPath_RejectsNilAADLegacyWraps asserts that a
// pre-Phase-D snapshot wrap (sealed with nil AAD, the old format) is rejected by
// LoadFromFSM, which now AAD-unwraps every wrap with DomainDEKFSMWrap bound to
// (gen, activeKEKVer). This is the snapshot-path guard: no extra format marker
// is needed — the AAD change IS the marker.
func TestGreenfieldGuard_SnapshotPath_RejectsNilAADLegacyWraps(t *testing.T) {
	kek := make([]byte, KEKSize)
	if _, err := rand.Read(kek); err != nil {
		t.Fatal(err)
	}
	cid := testClusterID()
	// Build a LEGACY (pre-Phase-D) wrap: sealed with nil AAD, the old format.
	legacyWrap, err := AESGCMSeal(kek, make([]byte, DEKSize))
	if err != nil {
		t.Fatal(err)
	}
	// LoadFromFSM now AAD-unwraps with DomainDEKFSMWrap(gen=0, kekVer=0); the
	// nil-AAD legacy wrap MUST fail authentication → restore errors → boot aborts.
	_, err = LoadFromFSM(kek, cid, map[uint32][]byte{0: legacyWrap}, 0)
	if err == nil {
		t.Fatal("LoadFromFSM must reject nil-AAD legacy snapshot wraps (greenfield snapshot-path guard)")
	}
}

// InstallReplicatedDEK is idempotent for identical bytes and rejects mismatched
// bytes for an already-installed gen.
func TestInstallReplicatedDEK_Idempotent(t *testing.T) {
	kek := make([]byte, KEKSize)
	rand.Read(kek)
	cid := testClusterID()
	leader, _ := NewDEKKeeper(kek, cid)
	wrapped, kekVer, _ := leader.GenerateWrappedDEK(1)

	f, _ := NewEmptyDEKKeeper(kek, cid)
	if err := f.InstallReplicatedDEK(1, wrapped, kekVer); err != nil {
		t.Fatalf("first install: %v", err)
	}
	// same bytes again -> no-op success
	if err := f.InstallReplicatedDEK(1, wrapped, kekVer); err != nil {
		t.Fatalf("idempotent install: %v", err)
	}
	// different bytes for the same gen -> error
	other, _, _ := leader.GenerateWrappedDEK(1)
	if bytes.Equal(other, wrapped) {
		t.Skip("two random DEKs collided (astronomically unlikely)")
	}
	if err := f.InstallReplicatedDEK(1, other, kekVer); err == nil {
		t.Fatal("install of mismatched bytes for existing gen must error")
	}
}

// LoadFromFSM re-unwraps persisted wraps under AAD (gen, activeKEKVer) and
// reconstructs a working keeper.
func TestLoadFromFSM_AADBound(t *testing.T) {
	kek := make([]byte, KEKSize)
	rand.Read(kek)
	cid := testClusterID()

	src, _ := NewDEKKeeper(kek, cid)
	versions, active := src.VersionsAndActive()
	if active != 0 {
		t.Fatalf("active=%d want 0", active)
	}

	restored, err := LoadFromFSM(kek, cid, versions, 0)
	if err != nil {
		t.Fatalf("LoadFromFSM: %v", err)
	}
	ct, gen, _ := src.Seal([]byte("restore-me"))
	got, err := restored.Open(ct, gen)
	if err != nil || string(got) != "restore-me" {
		t.Fatalf("restored decrypt: got=%q err=%v", got, err)
	}
	if restored.ActiveKEKVersion() != 0 {
		t.Fatalf("restored ActiveKEKVersion=%d want 0", restored.ActiveKEKVersion())
	}

	// Wrong activeKEKVer parameter fails AAD auth.
	if _, err := LoadFromFSM(kek, cid, versions, 1); err == nil {
		t.Fatal("LoadFromFSM with wrong activeKEKVer must fail AAD auth")
	}
}
