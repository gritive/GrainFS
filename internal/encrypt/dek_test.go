package encrypt

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestDEKKeeper_GenerateInitial(t *testing.T) {
	kek := make([]byte, 32)
	rand.Read(kek)
	k, err := NewDEKKeeper(kek)
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	if gen, _ := k.Active(); gen != 0 {
		t.Fatalf("active gen = %d, want 0 on empty bootstrap", gen)
	}
}

func TestDEKKeeper_SealOpenRoundTrip(t *testing.T) {
	kek := make([]byte, 32)
	rand.Read(kek)
	k, _ := NewDEKKeeper(kek)
	plain := []byte("hello dek")
	ct, gen, err := k.Seal(plain)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if gen != 0 {
		t.Fatalf("gen = %d, want 0", gen)
	}
	got, err := k.Open(ct, gen)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("roundtrip mismatch")
	}
}

func TestDEKKeeper_RotateBumpsGen(t *testing.T) {
	kek := make([]byte, 32)
	rand.Read(kek)
	k, _ := NewDEKKeeper(kek)
	ct0, gen0, _ := k.Seal([]byte("v0"))
	if gen0 != 0 {
		t.Fatalf("gen0 = %d, want 0", gen0)
	}
	if err := k.Rotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	ct1, gen1, _ := k.Seal([]byte("v1"))
	if gen1 != 1 {
		t.Fatalf("gen1 = %d, want 1", gen1)
	}
	// reads of both generations work
	if got, _ := k.Open(ct0, 0); !bytes.Equal(got, []byte("v0")) {
		t.Fatalf("read gen 0 failed")
	}
	if got, _ := k.Open(ct1, 1); !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("read gen 1 failed")
	}
}

func TestDEKKeeper_PruneRefusedWhenStillReferenced(t *testing.T) {
	kek := make([]byte, 32)
	rand.Read(kek)
	k, _ := NewDEKKeeper(kek)
	k.Rotate() // now have gen 0 and 1
	// gen 0 has not been rewrapped — prune must refuse
	if err := k.Prune(0 /*safe=*/, false); err == nil {
		t.Fatalf("Prune(0, safe=false) succeeded; expected refusal")
	}
	if err := k.Prune(0 /*safe=*/, true); err != nil {
		t.Fatalf("Prune(0, safe=true) failed: %v", err)
	}
}

func TestDEKKeeper_PruneRefusesActiveGen(t *testing.T) {
	kek := make([]byte, 32)
	rand.Read(kek)
	k, _ := NewDEKKeeper(kek)
	_ = k.Rotate() // active = 1
	if err := k.Prune(1, true); err == nil {
		t.Fatal("Prune(active_gen, true) must refuse — would lose ability to seal new objects")
	}
}

func TestLoadFromFSM_EmptyVersions(t *testing.T) {
	kek := make([]byte, 32)
	rand.Read(kek)
	if _, err := LoadFromFSM(kek, nil); err == nil {
		t.Fatal("LoadFromFSM(nil) must reject")
	}
	if _, err := LoadFromFSM(kek, map[uint32][]byte{}); err == nil {
		t.Fatal("LoadFromFSM(empty map) must reject")
	}
}

func TestLoadFromFSM_RoundTrip(t *testing.T) {
	kek := make([]byte, 32)
	rand.Read(kek)
	original, _ := NewDEKKeeper(kek)
	_ = original.Rotate()
	_ = original.Rotate() // gens 0, 1, 2 active=2

	restored, err := LoadFromFSM(kek, original.Versions())
	if err != nil {
		t.Fatalf("LoadFromFSM: %v", err)
	}
	if gen, _ := restored.Active(); gen != 2 {
		t.Fatalf("restored active gen = %d, want 2", gen)
	}
	// Seal+Open across the keepers must produce identical results (same KEK + same wrapped DEKs).
	ct, gen, err := original.Seal([]byte("crossed"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	got, err := restored.Open(ct, gen)
	if err != nil {
		t.Fatalf("restored.Open: %v", err)
	}
	if !bytes.Equal(got, []byte("crossed")) {
		t.Fatalf("payload mismatch: %q", got)
	}
}

func TestDEKKeeper_VersionsIsDeepCopy(t *testing.T) {
	kek := make([]byte, 32)
	rand.Read(kek)
	k, _ := NewDEKKeeper(kek)
	got := k.Versions()
	// Zero the returned bytes for active gen; subsequent Seal must still work.
	for g := range got {
		for i := range got[g] {
			got[g][i] = 0
		}
	}
	if _, _, err := k.Seal([]byte("after-mutate")); err != nil {
		t.Fatalf("Versions() returned a reference, not a copy; Seal failed: %v", err)
	}
}
