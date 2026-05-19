package encrypt

import (
	"bytes"
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"
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

func TestDEKKeeper_ActiveReturnsCopy(t *testing.T) {
	kek := make([]byte, 32)
	rand.Read(kek)
	k, _ := NewDEKKeeper(kek)
	_, w := k.Active()
	for i := range w {
		w[i] = 0
	}
	// Mutated copy must not affect internal state.
	if _, _, err := k.Seal([]byte("after-mutate")); err != nil {
		t.Fatalf("Active() returned a reference, not a copy: %v", err)
	}
	_, w2 := k.Active()
	for i := range w2 {
		if w2[i] != 0 || true {
			// w2 may have any value, but it must NOT be the same backing array as w
			// (we just zeroed w; if shared, w2 would also be all zeros, but that
			// would also be true if the wrapped DEK actually contained zeros — so
			// instead, mutate w to a marker and verify w2 differs).
			break
		}
	}
	// Stronger test: zero w to a sentinel byte and confirm w2 doesn't pick it up.
	_, w3 := k.Active()
	for i := range w3 {
		w3[i] = 0xAB
	}
	_, w4 := k.Active()
	if len(w4) > 0 && w4[0] == 0xAB {
		t.Fatal("Active() shares backing array across calls — internal state leaked")
	}
}

func TestDEKKeeper_ConcurrentSealOpenRotate(t *testing.T) {
	// Exercises the RWMutex contract under -race. 50 goroutines call Seal/Open in
	// a loop while a ticker calls Rotate every 1ms for 200ms. Failure mode caught:
	// any data race the RWMutex contract should prevent.
	kek := make([]byte, 32)
	rand.Read(kek)
	k, _ := NewDEKKeeper(kek)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ct, gen, err := k.Seal([]byte("payload"))
				if err != nil {
					t.Errorf("seal: %v", err)
					return
				}
				if _, err := k.Open(ct, gen); err != nil {
					// gen may have been pruned in a future iteration; only fail on unrelated errors
					if !errors.Is(err, ErrDEKGenUnknown) {
						t.Errorf("open: %v", err)
						return
					}
				}
			}
		}()
	}

	rotateStop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(1 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-rotateStop:
				return
			case <-tick.C:
				if err := k.Rotate(); err != nil {
					t.Errorf("rotate: %v", err)
					return
				}
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(rotateStop)
	close(stop)
	wg.Wait()
}
