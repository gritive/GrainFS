package cluster

import (
	"crypto/ed25519"
	"testing"
)

func TestMintInviteKeypair_UniqueAndUsable(t *testing.T) {
	priv1, pub1, id1, err := MintInviteKeypair()
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if len(pub1) != ed25519.PublicKeySize || len(priv1) != ed25519.PrivateKeySize {
		t.Fatalf("bad key sizes priv=%d pub=%d", len(priv1), len(pub1))
	}
	if len(id1) == 0 {
		t.Fatal("empty invite id")
	}
	msg := []byte("transcript")
	sig := ed25519.Sign(priv1, msg)
	if !ed25519.Verify(pub1, msg, sig) {
		t.Fatal("minted keypair does not sign/verify")
	}
	_, pub2, id2, _ := MintInviteKeypair()
	if string(pub1) == string(pub2) || id1 == id2 {
		t.Fatal("two mints collided")
	}
}

func TestInviteBundle_Roundtrip(t *testing.T) {
	priv, _, id, _ := MintInviteKeypair()
	var seed [32]byte
	seed[0] = 9
	b := EncodeInviteBundle(InviteBundle{
		InvitePriv: priv, InviteID: id, ClusterID: "cluster-x", SeedSPKI: seed,
	})
	got, err := DecodeInviteBundle(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.InviteID != id || got.ClusterID != "cluster-x" || got.SeedSPKI != seed {
		t.Fatal("bundle roundtrip mismatch")
	}
	if string(got.InvitePriv) != string(priv) {
		t.Fatal("bundle priv key roundtrip mismatch")
	}
}
