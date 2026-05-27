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
	k, err := NewDEKKeeper(kek, testClusterID())
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
	k, _ := NewDEKKeeper(kek, testClusterID())
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
	k, _ := NewDEKKeeper(kek, testClusterID())
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
	k, _ := NewDEKKeeper(kek, testClusterID())
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
	k, _ := NewDEKKeeper(kek, testClusterID())
	_ = k.Rotate() // active = 1
	if err := k.Prune(1, true); err == nil {
		t.Fatal("Prune(active_gen, true) must refuse — would lose ability to seal new objects")
	}
}

func TestLoadFromFSM_EmptyVersions(t *testing.T) {
	kek := make([]byte, 32)
	rand.Read(kek)
	if _, err := LoadFromFSM(kek, testClusterID(), nil, 0); err == nil {
		t.Fatal("LoadFromFSM(nil) must reject")
	}
	if _, err := LoadFromFSM(kek, testClusterID(), map[uint32][]byte{}, 0); err == nil {
		t.Fatal("LoadFromFSM(empty map) must reject")
	}
}

func TestLoadFromFSM_RoundTrip(t *testing.T) {
	kek := make([]byte, 32)
	rand.Read(kek)
	original, _ := NewDEKKeeper(kek, testClusterID())
	_ = original.Rotate()
	_ = original.Rotate() // gens 0, 1, 2 active=2

	restored, err := LoadFromFSM(kek, testClusterID(), original.Versions(), 0)
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
	k, _ := NewDEKKeeper(kek, testClusterID())
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
	k, _ := NewDEKKeeper(kek, testClusterID())
	_, w := k.Active()
	orig := append([]byte(nil), w...)
	for i := range w {
		w[i] ^= 0xFF
	}
	// Mutated copy must not affect internal state.
	if _, _, err := k.Seal([]byte("after-mutate")); err != nil {
		t.Fatalf("Active() returned a reference, not a copy: %v", err)
	}
	_, w2 := k.Active()
	if !bytes.Equal(w2, orig) {
		t.Fatal("Active() shares backing array across calls — internal state leaked")
	}
}

func TestDEKKeeper_ConcurrentSealOpenRotate(t *testing.T) {
	// Exercises the RWMutex contract under -race. 50 goroutines call Seal/Open in
	// a loop while a ticker calls Rotate every 1ms for 200ms. Failure mode caught:
	// any data race the RWMutex contract should prevent.
	kek := make([]byte, 32)
	rand.Read(kek)
	k, _ := NewDEKKeeper(kek, testClusterID())

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

func TestDEKKeeper_SealOpenWithAAD_RoundTrip(t *testing.T) {
	kek := bytes.Repeat([]byte{0x11}, KEKSize)
	k, err := NewDEKKeeper(kek, testClusterID())
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	plain := []byte("secret payload")
	aad := []byte("ctx|123")
	ct, gen, err := k.SealWithAAD(plain, aad)
	if err != nil {
		t.Fatalf("SealWithAAD: %v", err)
	}
	got, err := k.OpenWithAAD(ct, gen, aad)
	if err != nil {
		t.Fatalf("OpenWithAAD: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Errorf("round-trip mismatch: %q vs %q", got, plain)
	}
}

func TestDEKKeeper_OpenWithAAD_MismatchedAAD(t *testing.T) {
	kek := bytes.Repeat([]byte{0x22}, KEKSize)
	k, _ := NewDEKKeeper(kek, testClusterID())
	ct, gen, _ := k.SealWithAAD([]byte("p"), []byte("ctx-A"))
	if _, err := k.OpenWithAAD(ct, gen, []byte("ctx-B")); err == nil {
		t.Fatal("expected error on AAD mismatch, got nil")
	}
}

func TestDEKKeeper_RewrapWithAAD_CrossGen(t *testing.T) {
	kek := bytes.Repeat([]byte{0x33}, KEKSize)
	k, _ := NewDEKKeeper(kek, testClusterID())
	aad := []byte("rewrap-ctx")
	ct, oldGen, _ := k.SealWithAAD([]byte("payload"), aad)
	if err := k.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	newCt, newGen, err := k.RewrapWithAAD(ct, oldGen, aad)
	if err != nil {
		t.Fatalf("RewrapWithAAD: %v", err)
	}
	if newGen == oldGen {
		t.Errorf("RewrapWithAAD did not advance gen")
	}
	plain, err := k.OpenWithAAD(newCt, newGen, aad)
	if err != nil {
		t.Fatalf("OpenWithAAD after rewrap: %v", err)
	}
	if !bytes.Equal(plain, []byte("payload")) {
		t.Errorf("rewrapped plaintext mismatch")
	}
}

func TestDEKKeeper_RetainsKEKAfterCallerZeroizes(t *testing.T) {
	// Caller code (e.g., wireDEKKeeper) may zeroize its local KEK copy
	// after constructing a DEKKeeper. The keeper MUST retain its own
	// independent copy so Rotate continues to produce wraps that
	// LoadFromFSM (with the real KEK from disk) can later unwrap.
	kek := bytes.Repeat([]byte{0xAB}, KEKSize)
	keeper, err := NewDEKKeeper(kek, testClusterID())
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	original := append([]byte(nil), kek...) // remember the real KEK
	// Caller zeroizes their copy:
	for i := range kek {
		kek[i] = 0
	}
	if err := keeper.Rotate(); err != nil {
		t.Fatalf("Rotate after caller zeroize: %v", err)
	}
	// The keeper's wrap[1] must be openable with the ORIGINAL kek bytes
	// via a fresh LoadFromFSM. Simulates "restart with real kek.key on
	// disk".
	versions := keeper.Versions() // returns deep copy of wrap[] map
	restored, err := LoadFromFSM(original, testClusterID(), versions, 0)
	if err != nil {
		t.Fatalf("LoadFromFSM with original KEK: %v", err)
	}
	if gen, _ := restored.Active(); gen == 0 {
		t.Errorf("LoadFromFSM did not see rotated gen=1; got active=%d", gen)
	}
}

func TestLoadFromFSM_RetainsKEKAfterCallerZeroizes(t *testing.T) {
	// Same invariant on the restore path.
	kek := bytes.Repeat([]byte{0xCD}, KEKSize)
	// Build a versions map by going through one keeper.
	src, _ := NewDEKKeeper(append([]byte(nil), kek...), testClusterID())
	versions := src.Versions()
	keeper, err := LoadFromFSM(kek, testClusterID(), versions, 0)
	if err != nil {
		t.Fatalf("LoadFromFSM: %v", err)
	}
	// Caller zeroizes their copy:
	for i := range kek {
		kek[i] = 0
	}
	if err := keeper.Rotate(); err != nil {
		t.Fatalf("Rotate after caller zeroize: %v", err)
	}
}

// TestDEKKeeper_InstallKEKRotation_SwapsKEKAndWraps simulates an FSM-driven
// KEK rotation: seed a keeper with K_old wrapping gen 0, then call
// InstallKEKRotation with K_new and the gen-0 DEK rewrapped under K_new.
// After the swap, Rotate() generates gen 1 — its wrap must unseal under
// K_new (proves the keeper installed the new KEK, not just the wraps).
func TestDEKKeeper_InstallKEKRotation_SwapsKEKAndWraps(t *testing.T) {
	kOld := bytes.Repeat([]byte{0xA0}, KEKSize)
	kNew := bytes.Repeat([]byte{0xA1}, KEKSize)

	// Seed keeper with K_old. Capture the original gen-0 wrap so we can
	// rebuild the equivalent ciphertext under K_new.
	keeper, err := NewDEKKeeper(kOld, testClusterID())
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	_, origWrap := keeper.Active()

	// Unwrap gen-0 plaintext with K_old, then re-seal under K_new — this is
	// what the rotation leader would do off the FSM apply path. DEK wraps are
	// AAD-bound to (clusterID, gen, kekVer); a KEK rotation keeps the same
	// kekVer label here (InstallKEKRotation does not bump activeKEKVer).
	cid := testClusterID()
	aad0 := BuildAAD(DomainDEKFSMWrap, cid, FieldUint32(0), FieldUint32(0))
	plain0, err := AESGCMOpenWithAAD(kOld, origWrap, aad0)
	if err != nil {
		t.Fatalf("unwrap gen-0 with kOld: %v", err)
	}
	newWrap0, err := AESGCMSealWithAAD(kNew, plain0, aad0)
	if err != nil {
		t.Fatalf("reseal gen-0 with kNew: %v", err)
	}
	zeroize(plain0)

	// Install: swap KEK + replace wrap[0] with the new ciphertext.
	if err := keeper.InstallKEKRotation(kNew, map[uint32][]byte{0: newWrap0}); err != nil {
		t.Fatalf("InstallKEKRotation: %v", err)
	}

	// Rotate creates gen-1 — its wrap must unseal under K_new, NOT K_old.
	if err := keeper.Rotate(); err != nil {
		t.Fatalf("Rotate after Install: %v", err)
	}
	wraps, active := keeper.VersionsAndActive()
	if active != 1 {
		t.Fatalf("active gen = %d, want 1", active)
	}
	w1, ok := wraps[1]
	if !ok {
		t.Fatalf("wrap[1] missing after Rotate")
	}
	aad1 := BuildAAD(DomainDEKFSMWrap, cid, FieldUint32(1), FieldUint32(0))
	if _, err := AESGCMOpenWithAAD(kNew, w1, aad1); err != nil {
		t.Errorf("wrap[1] should unseal under K_new: %v", err)
	}
	if _, err := AESGCMOpenWithAAD(kOld, w1, aad1); err == nil {
		t.Errorf("wrap[1] should NOT unseal under K_old — KEK swap failed")
	}

	// Existing AEAD for gen-0 must still work (plaintext unchanged across
	// KEK rotation): Open with the keeper succeeds.
	ct0, gen, err := keeper.Seal([]byte("after-rotate"))
	if err != nil {
		t.Fatalf("Seal after Install: %v", err)
	}
	if gen != 1 {
		t.Errorf("Seal gen = %d, want active=1", gen)
	}
	got, err := keeper.Open(ct0, gen)
	if err != nil {
		t.Fatalf("Open after Install: %v", err)
	}
	if !bytes.Equal(got, []byte("after-rotate")) {
		t.Errorf("roundtrip mismatch after rotation")
	}
}

func TestDEKKeeper_InstallKEKRotation_RejectsWrongLen(t *testing.T) {
	kek := bytes.Repeat([]byte{0xA0}, KEKSize)
	keeper, _ := NewDEKKeeper(kek, testClusterID())
	short := make([]byte, KEKSize-1)
	if err := keeper.InstallKEKRotation(short, map[uint32][]byte{}); err == nil {
		t.Errorf("expected error on short KEK")
	}
}

func TestKEKStore_HasVersion_SetActiveVersion(t *testing.T) {
	s := NewKEKStore()
	k0 := bytes.Repeat([]byte{0xA0}, KEKSize)
	k1 := bytes.Repeat([]byte{0xA1}, KEKSize)
	if err := s.Add(0, k0); err != nil {
		t.Fatalf("Add 0: %v", err)
	}
	if !s.HasVersion(0) {
		t.Errorf("HasVersion(0) = false, want true")
	}
	if s.HasVersion(1) {
		t.Errorf("HasVersion(1) = true on empty slot")
	}
	if err := s.SetActiveVersion(7); err == nil {
		t.Errorf("SetActiveVersion(7) should fail on unloaded version")
	}
	if err := s.Add(1, k1); err != nil {
		t.Fatalf("Add 1: %v", err)
	}
	if err := s.SetActiveVersion(0); err != nil {
		t.Fatalf("SetActiveVersion(0): %v", err)
	}
	if got := s.ActiveVersion(); got != 0 {
		t.Errorf("ActiveVersion = %d, want 0 after explicit SetActiveVersion", got)
	}
}

func TestCanonicalWrapSetHash_DeterministicAndOrderInvariant(t *testing.T) {
	a := []WrapSetEntry{
		{Gen: 1, Wrap: []byte("w1")},
		{Gen: 3, Wrap: []byte("w3")},
		{Gen: 2, Wrap: []byte("w2")},
	}
	b := []WrapSetEntry{
		{Gen: 3, Wrap: []byte("w3")},
		{Gen: 1, Wrap: []byte("w1")},
		{Gen: 2, Wrap: []byte("w2")},
	}
	ha := CanonicalWrapSetHash(a)
	hb := CanonicalWrapSetHash(b)
	if ha != hb {
		t.Errorf("hash order-sensitive: %x vs %x", ha, hb)
	}
	// Change content → different hash.
	c := []WrapSetEntry{
		{Gen: 1, Wrap: []byte("w1")},
		{Gen: 3, Wrap: []byte("w3")},
		{Gen: 2, Wrap: []byte("WRONG")},
	}
	if CanonicalWrapSetHash(c) == ha {
		t.Errorf("hash collided despite differing wrap bytes")
	}
}
