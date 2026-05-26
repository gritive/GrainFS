package encrypt

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"testing"
)

func mustEd(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return pub, priv
}

func sampleTranscript() InviteTranscript {
	return InviteTranscript{
		ClusterID: []byte("cid-0123456789ab"),
		Nonce:     bytes.Repeat([]byte{7}, 32),
		NodeID:    "node-a",
		Address:   "10.0.0.2:9000",
		SPKI:      bytes.Repeat([]byte{3}, 32),
		Bind:      bytes.Repeat([]byte{5}, 32),
	}
}

func TestInviteTranscriptSign_AcceptReject(t *testing.T) {
	pub, priv := mustEd(t)
	tr := sampleTranscript()
	sig := SignInviteTranscript(priv, tr)
	if !VerifyInviteTranscript(pub, tr, sig) {
		t.Fatal("valid signature rejected")
	}
	for _, mut := range []func(*InviteTranscript){
		func(x *InviteTranscript) { x.SPKI = bytes.Repeat([]byte{9}, 32) },
		func(x *InviteTranscript) { x.Bind = bytes.Repeat([]byte{9}, 32) },
		func(x *InviteTranscript) { x.NodeID = "node-evil" },
		func(x *InviteTranscript) { x.Nonce = bytes.Repeat([]byte{1}, 32) },
	} {
		bad := tr
		mut(&bad)
		if VerifyInviteTranscript(pub, bad, sig) {
			t.Fatal("tampered transcript accepted")
		}
	}
	otherPub, _ := mustEd(t)
	if VerifyInviteTranscript(otherPub, tr, sig) {
		t.Fatal("wrong invite public key accepted")
	}
}

func TestNodeTranscriptSign_AcceptReject(t *testing.T) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tr := sampleTranscript()
	sig, err := SignNodeTranscript(priv, tr)
	if err != nil {
		t.Fatal(err)
	}
	if !VerifyNodeTranscript(&priv.PublicKey, tr, sig) {
		t.Fatal("valid node signature rejected")
	}
	bad := tr
	bad.SPKI = bytes.Repeat([]byte{9}, 32)
	if VerifyNodeTranscript(&priv.PublicKey, bad, sig) {
		t.Fatal("tampered transcript accepted by node verify")
	}
	other, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if VerifyNodeTranscript(&other.PublicKey, tr, sig) {
		t.Fatal("wrong node public key accepted")
	}
}
