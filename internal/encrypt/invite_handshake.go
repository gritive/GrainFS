package encrypt

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
)

// InviteTranscript is the canonical material both InviteSig (Ed25519 invite key)
// and NodeSig (ECDSA per-node key) sign. It binds SPKI and the TLS exporter value
// so a relayed handshake on a different TLS session fails (spec §4.2B).
type InviteTranscript struct {
	ClusterID []byte
	Nonce     []byte // server-issued
	NodeID    string
	Address   string
	SPKI      []byte // sha256(SubjectPublicKeyInfo) of the joiner
	Bind      []byte // RFC 5705 TLS exporter value, label "grainfs-join-binding"
}

// CanonicalInviteTranscript returns the length-prefixed canonical digest signed
// by both InviteSig and NodeSig. Uses the same writeLenPrefixed helper as
// computeMACFromKEK (kek_handshake.go).
func CanonicalInviteTranscript(t InviteTranscript) []byte {
	h := sha256.New()
	h.Write([]byte("grainfs-invite-v1"))
	writeLenPrefixed(h, t.ClusterID)
	writeLenPrefixed(h, t.Nonce)
	writeLenPrefixed(h, []byte(t.NodeID))
	writeLenPrefixed(h, []byte(t.Address))
	writeLenPrefixed(h, t.SPKI)
	writeLenPrefixed(h, t.Bind)
	return h.Sum(nil)
}

// SignInviteTranscript signs the canonical transcript with the invite Ed25519
// private key (proves invite possession).
func SignInviteTranscript(priv ed25519.PrivateKey, t InviteTranscript) []byte {
	return ed25519.Sign(priv, CanonicalInviteTranscript(t))
}

// VerifyInviteTranscript verifies an Ed25519 InviteSig against the invite public key.
func VerifyInviteTranscript(pub ed25519.PublicKey, t InviteTranscript, sig []byte) bool {
	return ed25519.Verify(pub, CanonicalInviteTranscript(t), sig)
}

// SignNodeTranscript signs the canonical transcript with the joiner's per-node
// ECDSA (P-256) private key (Phase-1 identity). Returns an ASN.1 signature.
func SignNodeTranscript(priv *ecdsa.PrivateKey, t InviteTranscript) ([]byte, error) {
	return ecdsa.SignASN1(rand.Reader, priv, CanonicalInviteTranscript(t))
}

// VerifyNodeTranscript verifies an ECDSA NodeSig against the per-node public key
// (extracted from the joiner's presented cert by the caller). NOTE: the caller
// must independently confirm sha256(cert SPKI) == the claimed SPKI before trusting
// this — this function only checks the signature math.
func VerifyNodeTranscript(pub *ecdsa.PublicKey, t InviteTranscript, sig []byte) bool {
	return ecdsa.VerifyASN1(pub, CanonicalInviteTranscript(t), sig)
}
