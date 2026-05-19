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
