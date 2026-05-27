package encrypt

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"testing"
)

func mustKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return k
}

func TestSealToPeer_RoundTrip(t *testing.T) {
	recipient := mustKey(t)
	plain := []byte("cluster-secret-bytes")
	ctx := []byte("grainfs-bootstrap-v1|cluster-A|invite-1|node-2|leader-1")
	aad := ctx

	blob, err := SealToPeer(&recipient.PublicKey, plain, ctx, aad)
	if err != nil {
		t.Fatalf("SealToPeer: %v", err)
	}
	if len(blob.EphemeralPub) == 0 || len(blob.Ciphertext) == 0 {
		t.Fatalf("empty blob fields: %+v", blob)
	}

	got, err := OpenFromPeer(recipient, blob, ctx, aad)
	if err != nil {
		t.Fatalf("OpenFromPeer: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("round-trip mismatch: got %q want %q", got, plain)
	}
}

func TestSealToPeer_WrongRecipientFails(t *testing.T) {
	a, b := mustKey(t), mustKey(t)
	ctx := []byte("ctx")
	blob, err := SealToPeer(&a.PublicKey, []byte("secret"), ctx, ctx)
	if err != nil {
		t.Fatalf("SealToPeer: %v", err)
	}
	if _, err := OpenFromPeer(b, blob, ctx, ctx); err == nil {
		t.Fatal("expected open with wrong key to fail, got nil error")
	}
}

func TestSealToPeer_TamperedAADFails(t *testing.T) {
	k := mustKey(t)
	ctx := []byte("ctx")
	blob, err := SealToPeer(&k.PublicKey, []byte("secret"), ctx, []byte("aad-1"))
	if err != nil {
		t.Fatalf("SealToPeer: %v", err)
	}
	if _, err := OpenFromPeer(k, blob, ctx, []byte("aad-2")); err == nil {
		t.Fatal("expected open with mismatched AAD to fail, got nil error")
	}
}

func TestSealToPeer_TamperedCiphertextFails(t *testing.T) {
	k := mustKey(t)
	ctx := []byte("ctx")
	blob, err := SealToPeer(&k.PublicKey, []byte("secret"), ctx, ctx)
	if err != nil {
		t.Fatalf("SealToPeer: %v", err)
	}
	blob.Ciphertext[len(blob.Ciphertext)-1] ^= 0xFF
	if _, err := OpenFromPeer(k, blob, ctx, ctx); err == nil {
		t.Fatal("expected open with tampered ciphertext to fail, got nil error")
	}
}

// TestSealToPeer_TamperedEphemeralPubFails documents that EphemeralPub is
// implicitly authenticated: replacing it with another valid P-256 point changes
// the recipient's derived shared secret, so the GCM tag fails to verify. This
// guards the implicit-binding property the SealToPeer godoc relies on.
func TestSealToPeer_TamperedEphemeralPubFails(t *testing.T) {
	k := mustKey(t)
	ctx := []byte("ctx")
	blob, err := SealToPeer(&k.PublicKey, []byte("secret"), ctx, ctx)
	if err != nil {
		t.Fatalf("SealToPeer: %v", err)
	}
	other, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate other ephemeral: %v", err)
	}
	blob.EphemeralPub = other.PublicKey().Bytes()
	if _, err := OpenFromPeer(k, blob, ctx, ctx); err == nil {
		t.Fatal("expected open with substituted ephemeral pub to fail, got nil error")
	}
}
