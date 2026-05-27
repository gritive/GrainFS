package encrypt

import (
	"bytes"
	"testing"
)

func newTransitionKeeper(t *testing.T) *DEKKeeper {
	t.Helper()
	kek := bytes.Repeat([]byte{0x07}, KEKSize)
	k, err := NewDEKKeeper(kek, testClusterID())
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	return k
}

func TestTransitionReseal_RoundTrip(t *testing.T) {
	k := newTransitionKeeper(t)
	clusterID := testClusterID()

	plain := []byte("canonical chunk plaintext")
	legacyAAD := []byte("bucket/key/0") // stand-in for shardAAD(...)
	casAAD := CASChunkAAD(clusterID, []byte("blake3-locator"))

	ct0, gen0, err := k.SealWithAAD(plain, legacyAAD)
	if err != nil {
		t.Fatalf("SealWithAAD: %v", err)
	}

	if err := k.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	ct1, gen1, err := k.TransitionReseal(ct0, gen0, legacyAAD, casAAD)
	if err != nil {
		t.Fatalf("TransitionReseal: %v", err)
	}
	if gen1 == gen0 {
		t.Fatalf("re-seal must use the active generation, got same gen %d", gen1)
	}

	got, err := k.OpenWithAAD(ct1, gen1, casAAD)
	if err != nil {
		t.Fatalf("OpenWithAAD(casAAD): %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("plaintext mismatch: got %q want %q", got, plain)
	}
}

func TestTransitionReseal_LegacyAADNoLongerOpens(t *testing.T) {
	k := newTransitionKeeper(t)
	clusterID := testClusterID()
	plain := []byte("payload")
	legacyAAD := []byte("bucket/key/0")
	casAAD := CASChunkAAD(clusterID, []byte("loc"))

	ct0, gen0, err := k.SealWithAAD(plain, legacyAAD)
	if err != nil {
		t.Fatalf("SealWithAAD: %v", err)
	}
	ct1, gen1, err := k.TransitionReseal(ct0, gen0, legacyAAD, casAAD)
	if err != nil {
		t.Fatalf("TransitionReseal: %v", err)
	}
	if _, err := k.OpenWithAAD(ct1, gen1, legacyAAD); err == nil {
		t.Fatal("expected auth failure opening CAS chunk with legacy AAD")
	}
}

func TestTransitionReseal_WrongOldAADFails(t *testing.T) {
	k := newTransitionKeeper(t)
	clusterID := testClusterID()
	plain := []byte("payload")
	legacyAAD := []byte("bucket/key/0")
	casAAD := CASChunkAAD(clusterID, []byte("loc"))

	ct0, gen0, err := k.SealWithAAD(plain, legacyAAD)
	if err != nil {
		t.Fatalf("SealWithAAD: %v", err)
	}
	if _, _, err := k.TransitionReseal(ct0, gen0, []byte("wrong/aad/0"), casAAD); err == nil {
		t.Fatal("expected auth failure with wrong oldAAD")
	}
}

func TestTransitionReseal_UnknownGen(t *testing.T) {
	k := newTransitionKeeper(t)
	_, _, err := k.TransitionReseal([]byte("0123456789ab....."), 99, []byte("a"), []byte("b"))
	if err != ErrDEKGenUnknown {
		t.Fatalf("expected ErrDEKGenUnknown, got %v", err)
	}
}

func TestTransitionReseal_CiphertextTooShort(t *testing.T) {
	k := newTransitionKeeper(t)
	_, _, err := k.TransitionReseal([]byte{0x00}, 0, []byte("a"), []byte("b"))
	if err != ErrCiphertextTooShort {
		t.Fatalf("expected ErrCiphertextTooShort, got %v", err)
	}
}
