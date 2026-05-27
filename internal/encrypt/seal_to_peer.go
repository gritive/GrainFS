package encrypt

import (
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"

	"golang.org/x/crypto/hkdf"
)

// SealedToPeer is the wire form of a SealToPeer result: the per-call ephemeral
// P-256 public key (uncompressed point bytes) plus the AES-256-GCM ciphertext
// (nonce-prefixed, as produced by aesgcmSealWithAAD). The ephemeral public key
// is not secret; it travels in the clear.
type SealedToPeer struct {
	EphemeralPub []byte
	Ciphertext   []byte
}

// SealToPeer encrypts plain so that ONLY the holder of peerPub's private key can
// open it. It performs ephemeral-static ECDH over P-256: a fresh ephemeral
// keypair is generated per call (forward secrecy w.r.t. the sender), the shared
// secret is expanded via HKDF-SHA256 with info=contextInfo into a 32-byte key,
// and plain is sealed with AES-256-GCM binding aad. Bind contextInfo/aad to the
// transcript identity (clusterID || inviteID || joinerNodeID || leaderNodeID ||
// kekGenList) so a blob cannot be replayed into a different context.
func SealToPeer(peerPub *ecdsa.PublicKey, plain, contextInfo, aad []byte) (SealedToPeer, error) {
	peerECDH, err := peerPub.ECDH()
	if err != nil {
		return SealedToPeer{}, fmt.Errorf("SealToPeer: peer key to ECDH: %w", err)
	}
	eph, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		return SealedToPeer{}, fmt.Errorf("SealToPeer: ephemeral key: %w", err)
	}
	shared, err := eph.ECDH(peerECDH)
	if err != nil {
		return SealedToPeer{}, fmt.Errorf("SealToPeer: ECDH: %w", err)
	}
	key := make([]byte, 32)
	if _, err := io.ReadFull(hkdf.New(sha256.New, shared, nil, contextInfo), key); err != nil {
		return SealedToPeer{}, fmt.Errorf("SealToPeer: hkdf: %w", err)
	}
	ct, err := aesgcmSealWithAAD(key, plain, aad)
	if err != nil {
		return SealedToPeer{}, fmt.Errorf("SealToPeer: seal: %w", err)
	}
	return SealedToPeer{EphemeralPub: eph.PublicKey().Bytes(), Ciphertext: ct}, nil
}

// OpenFromPeer reverses SealToPeer using the recipient's long-term ECDSA P-256
// private key. contextInfo and aad MUST match the values used to seal.
func OpenFromPeer(nodePriv *ecdsa.PrivateKey, blob SealedToPeer, contextInfo, aad []byte) ([]byte, error) {
	privECDH, err := nodePriv.ECDH()
	if err != nil {
		return nil, fmt.Errorf("OpenFromPeer: node key to ECDH: %w", err)
	}
	ephPub, err := ecdh.P256().NewPublicKey(blob.EphemeralPub)
	if err != nil {
		return nil, fmt.Errorf("OpenFromPeer: parse ephemeral pub: %w", err)
	}
	shared, err := privECDH.ECDH(ephPub)
	if err != nil {
		return nil, fmt.Errorf("OpenFromPeer: ECDH: %w", err)
	}
	key := make([]byte, 32)
	if _, err := io.ReadFull(hkdf.New(sha256.New, shared, nil, contextInfo), key); err != nil {
		return nil, fmt.Errorf("OpenFromPeer: hkdf: %w", err)
	}
	return aesgcmOpenWithAAD(key, blob.Ciphertext, aad)
}
