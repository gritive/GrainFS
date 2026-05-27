package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// TestGreenfieldGuard_RefusesLegacyType48Gens verifies that
// CheckGreenfieldDEKBoundary returns an error when a legacy type-48
// MetaCmdTypeDEKRotate entry was replayed.
func TestGreenfieldGuard_RefusesLegacyType48Gens(t *testing.T) {
	kek := make([]byte, encrypt.KEKSize)
	k, err := encrypt.NewEmptyDEKKeeper(kek, dekTestClusterID())
	if err != nil {
		t.Fatalf("NewEmptyDEKKeeper: %v", err)
	}
	fsm := newTestMetaFSMWithDEKKeeper(t, k)

	// Simulate a pre-Phase-D log: a type-48 apply with NO covering type-50
	// and NO snapshot-trailer gen. applyDEKRotate sets legacyDEKRotateSeen.
	if err := fsm.applyDEKRotate(); err != nil {
		t.Fatalf("applyDEKRotate: %v", err)
	}
	if err := fsm.CheckGreenfieldDEKBoundary(); err == nil {
		t.Fatal("must refuse boot: legacy type-48 gens with no trailer cover")
	}
}

// TestGreenfieldGuard_OKWhenNoLegacyAndGensFromTrailer verifies that
// CheckGreenfieldDEKBoundary returns nil when no type-48 was ever seen —
// i.e., this is a greenfield Phase-D cluster with gens installed from the
// snapshot trailer only.
func TestGreenfieldGuard_OKWhenNoLegacyAndGensFromTrailer(t *testing.T) {
	kek := make([]byte, encrypt.KEKSize)
	cid := dekTestClusterID()

	// Build a gen-0 keeper (simulates snapshot-trailer install via LoadFromFSM).
	src, err := encrypt.NewDEKKeeper(kek, cid)
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	versions, _ := src.VersionsAndActive()
	k, err := encrypt.LoadFromFSM(kek, cid, versions, 0)
	if err != nil {
		t.Fatalf("LoadFromFSM: %v", err)
	}

	fsm := newTestMetaFSMWithDEKKeeper(t, k)
	// No type-48 applied — legacyDEKRotateSeen is false.
	if err := fsm.CheckGreenfieldDEKBoundary(); err != nil {
		t.Fatalf("clean greenfield boot must pass: %v", err)
	}
}
