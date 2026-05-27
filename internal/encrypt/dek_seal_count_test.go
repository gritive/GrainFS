package encrypt

import (
	"crypto/rand"
	"testing"
)

func TestDEKKeeper_SealCount_IncrementsOnActiveSealPath(t *testing.T) {
	kek := make([]byte, 32)
	rand.Read(kek)
	k, _ := NewDEKKeeper(kek, testClusterID())

	// active DEK generation defaults to 0.
	if got := k.SealCount(0); got != 0 {
		t.Fatalf("initial seal count = %d, want 0", got)
	}

	if _, _, err := k.Seal([]byte("a")); err != nil {
		t.Fatalf("seal: %v", err)
	}
	if _, _, err := k.SealWithAAD([]byte("b"), []byte("aad")); err != nil {
		t.Fatalf("sealWithAAD: %v", err)
	}
	if got := k.SealCount(0); got != 2 {
		t.Fatalf("seal count = %d, want 2 after Seal+SealWithAAD", got)
	}
}

// A KEK rotation re-wraps the existing DEK without changing the key, so the
// per-DEK nonce count MUST persist across it. OnKEKRotation only re-labels the
// active KEK version; it must NOT freeze or reset the seal counter.
func TestDEKKeeper_SealCount_PersistsAcrossKEKRotation(t *testing.T) {
	kek := make([]byte, 32)
	rand.Read(kek)
	k, _ := NewDEKKeeper(kek, testClusterID())

	k.Seal([]byte("a"))
	k.Seal([]byte("b"))
	if got := k.SealCount(0); got != 2 {
		t.Fatalf("gen-0 seal count = %d, want 2", got)
	}

	// Simulate a KEK rotation to version 1: the DEK gen is unchanged (gen 0),
	// so the gen-0 seal count must carry over.
	newKEK := make([]byte, 32)
	rand.Read(newKEK)
	versions, _ := k.VersionsAndActive()
	if err := k.InstallKEKRotation(newKEK, versions); err != nil {
		t.Fatalf("install: %v", err)
	}
	k.OnKEKRotation(1)

	if got := k.ActiveKEKVersion(); got != 1 {
		t.Fatalf("active KEK version = %d, want 1 after rotation", got)
	}
	if got := k.SealCount(0); got != 2 {
		t.Fatalf("gen-0 seal count = %d, want 2 to PERSIST across KEK rotation", got)
	}
	// Another seal under the same DEK gen continues the same count.
	k.Seal([]byte("c"))
	if got := k.SealCount(0); got != 3 {
		t.Fatalf("gen-0 seal count = %d, want 3 after KEK rotation + 1 seal", got)
	}
}

// Installing a NEW DEK generation must freeze the prior gen's count and start
// the new gen at zero (a new DEK key = a fresh AES-GCM nonce space).
func TestDEKKeeper_SealCount_ResetsOnDEKGenAdvance(t *testing.T) {
	kek := make([]byte, 32)
	rand.Read(kek)
	k, _ := NewDEKKeeper(kek, testClusterID())

	k.Seal([]byte("a"))
	k.Seal([]byte("b"))
	if got := k.SealCount(0); got != 2 {
		t.Fatalf("gen-0 seal count = %d, want 2", got)
	}

	// Install gen-1 (a genuinely new generation). Build a wrapped gen-1 DEK as
	// the leader would, then install it as a follower does.
	wrapped, kekVer, err := k.GenerateWrappedDEK(1)
	if err != nil {
		t.Fatalf("generate gen-1: %v", err)
	}
	if err := k.InstallReplicatedDEK(1, wrapped, kekVer); err != nil {
		t.Fatalf("install gen-1: %v", err)
	}

	if got := k.SealCount(0); got != 2 {
		t.Fatalf("frozen gen-0 seal count = %d, want 2", got)
	}
	if got := k.SealCount(1); got != 0 {
		t.Fatalf("gen-1 seal count = %d, want 0 immediately after DEK rotation", got)
	}
	k.Seal([]byte("c"))
	if got := k.SealCount(1); got != 1 {
		t.Fatalf("gen-1 seal count = %d, want 1", got)
	}
	if got := k.SealCount(0); got != 2 {
		t.Fatalf("gen-0 seal count drifted to %d, want 2", got)
	}
}

// Idempotent re-install of the SAME generation+bytes (snapshot-tail replay)
// must NOT freeze or reset the running count.
func TestDEKKeeper_SealCount_IdempotentReinstallDoesNotReset(t *testing.T) {
	kek := make([]byte, 32)
	rand.Read(kek)
	k, _ := NewDEKKeeper(kek, testClusterID())

	wrapped, kekVer, err := k.GenerateWrappedDEK(1)
	if err != nil {
		t.Fatalf("generate gen-1: %v", err)
	}
	if err := k.InstallReplicatedDEK(1, wrapped, kekVer); err != nil {
		t.Fatalf("install gen-1: %v", err)
	}
	k.Seal([]byte("a"))
	k.Seal([]byte("b"))
	if got := k.SealCount(1); got != 2 {
		t.Fatalf("gen-1 seal count = %d, want 2", got)
	}

	// Re-install the identical gen-1 bytes (replay). Must be a no-op for the
	// seal counter.
	if err := k.InstallReplicatedDEK(1, wrapped, kekVer); err != nil {
		t.Fatalf("re-install gen-1: %v", err)
	}
	if got := k.SealCount(1); got != 2 {
		t.Fatalf("gen-1 seal count = %d after idempotent re-install, want 2 (no reset)", got)
	}
}

// Rotate (single-node path) also advances the DEK gen and must freeze + reset.
func TestDEKKeeper_SealCount_RotateFreezesAndResets(t *testing.T) {
	kek := make([]byte, 32)
	rand.Read(kek)
	k, _ := NewDEKKeeper(kek, testClusterID())

	k.Seal([]byte("a"))
	k.Seal([]byte("b"))
	k.Seal([]byte("c"))
	if got := k.SealCount(0); got != 3 {
		t.Fatalf("gen-0 seal count = %d, want 3", got)
	}

	if err := k.Rotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if got := k.SealCount(0); got != 3 {
		t.Fatalf("frozen gen-0 seal count = %d, want 3", got)
	}
	if got := k.SealCount(1); got != 0 {
		t.Fatalf("gen-1 seal count = %d, want 0 after Rotate", got)
	}
}

// SealCountSnapshot is keyed by DEK generation: the live count for the active
// gen plus the frozen count for every retired gen.
func TestDEKKeeper_SealCountSnapshot_KeyedByDEKGen(t *testing.T) {
	kek := make([]byte, 32)
	rand.Read(kek)
	k, _ := NewDEKKeeper(kek, testClusterID())

	k.Seal([]byte("a"))
	if err := k.Rotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	k.Seal([]byte("b"))
	k.Seal([]byte("c"))

	snap := k.SealCountSnapshot()
	if snap[0] != 1 {
		t.Fatalf("snapshot[gen0] = %d, want 1", snap[0])
	}
	if snap[1] != 2 {
		t.Fatalf("snapshot[gen1] = %d, want 2", snap[1])
	}
	if got := k.ActiveDEKGeneration(); got != 1 {
		t.Fatalf("active DEK generation = %d, want 1", got)
	}
}
