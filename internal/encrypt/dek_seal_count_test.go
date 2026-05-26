package encrypt

import (
	"crypto/rand"
	"testing"
)

func TestDEKKeeper_SealCount_IncrementsOnActiveSealPath(t *testing.T) {
	kek := make([]byte, 32)
	rand.Read(kek)
	k, _ := NewDEKKeeper(kek)

	// active KEK version defaults to 0.
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

func TestDEKKeeper_SealCount_ResetsOnKEKRotation(t *testing.T) {
	kek := make([]byte, 32)
	rand.Read(kek)
	k, _ := NewDEKKeeper(kek)

	k.Seal([]byte("a"))
	k.Seal([]byte("b"))
	if got := k.SealCount(0); got != 2 {
		t.Fatalf("v0 seal count = %d, want 2", got)
	}

	// Simulate a KEK rotation to version 1: the prior count freezes under v0
	// and the new active version starts its own counter.
	newKEK := make([]byte, 32)
	rand.Read(newKEK)
	versions, _ := k.VersionsAndActive()
	if err := k.InstallKEKRotation(newKEK, versions); err != nil {
		t.Fatalf("install: %v", err)
	}
	k.OnKEKRotation(1)

	if got := k.SealCount(0); got != 2 {
		t.Fatalf("frozen v0 seal count = %d, want 2", got)
	}
	if got := k.SealCount(1); got != 0 {
		t.Fatalf("v1 seal count = %d, want 0 immediately after rotation", got)
	}
	k.Seal([]byte("c"))
	if got := k.SealCount(1); got != 1 {
		t.Fatalf("v1 seal count = %d, want 1", got)
	}
	// v0 unchanged.
	if got := k.SealCount(0); got != 2 {
		t.Fatalf("v0 seal count drifted to %d, want 2", got)
	}
}
