package transport

import (
	"crypto/elliptic"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"testing"

	"golang.org/x/crypto/hkdf"
)

// Golden regression vectors pinning the deterministic cluster-identity
// derivation. These bytes feed the cluster's TLS identity (SPKI pinning), so
// ANY change to them would break cluster auth across nodes/versions. The
// vectors are characterization constants captured from the original
// ScalarBaseMult/priv.D construction; they MUST remain byte-identical after the
// ecdsa.ParseRawPrivateKey migration. If one differs, the migration changed the
// derived key — STOP, do not edit the constant to match.
const (
	// SPKI = sha256(SubjectPublicKeyInfo) of the self-signed cluster cert.
	// Public-key-only and time-independent, so stable across runs.
	goldenSPKIHex = "c8ddcbaaa06f8b52734978e8ccaec05bdd64a346661bd7e08b1066faa669b96b"

	// priv.Bytes() (Go 1.25 API) — the fixed-width big-endian scalar D.
	goldenPrivBytesHex = "2c0a755ddda39c3d808f16321ef3e301e7023e1b67d41aec7c999e24d5540675"

	// x509.MarshalPKIXPublicKey of the derived public key (encodes X,Y).
	goldenPKIXPubHex = "3059301306072a8648ce3d020106082a8648ce3d03010703420004626b27fa97c29b1e2b8e0504f480492c6da0af7c849e7d8ff53e9443b1dae6f1bbdf4a531e47d6f11e56ed4cd6ef9cf32a2d4e780fbd90f56142b7efe36b25dc"
)

// TestDeriveClusterIdentity_SPKIGoldenVector pins the full-path SPKI for a
// fixed PSK. This is the load-bearing cluster-identity invariant: same PSK must
// yield the same pinned SPKI on every node and every version.
func TestDeriveClusterIdentity_SPKIGoldenVector(t *testing.T) {
	_, spki, err := DeriveClusterIdentity("grainfs-golden-test-psk")
	if err != nil {
		t.Fatalf("DeriveClusterIdentity: %v", err)
	}
	got := hex.EncodeToString(spki[:])
	if got != goldenSPKIHex {
		t.Fatalf("SPKI drifted from golden:\n got  %s\n want %s\n"+
			"This breaks cluster SPKI pinning. Do NOT update the golden to match — "+
			"the derived key changed.", got, goldenSPKIHex)
	}
}

// TestDerivePrivKeyFromHKDF_KeyGoldenVector pins the tightest possible vectors
// on the derived key itself: the scalar D (priv.Bytes()) and the public point
// (X,Y via MarshalPKIXPublicKey), for a fixed HKDF reader. Uses SA1019-free
// accessors so the test does not itself depend on the deprecated D/X/Y fields.
func TestDerivePrivKeyFromHKDF_KeyGoldenVector(t *testing.T) {
	r := hkdf.New(sha256.New, []byte("golden-psk"), nil, []byte("golden-info"))
	priv, err := derivePrivKeyFromHKDF(r, elliptic.P256())
	if err != nil {
		t.Fatalf("derivePrivKeyFromHKDF: %v", err)
	}

	privBytes, err := priv.Bytes()
	if err != nil {
		t.Fatalf("priv.Bytes(): %v", err)
	}
	if got := hex.EncodeToString(privBytes); got != goldenPrivBytesHex {
		t.Fatalf("scalar D drifted from golden:\n got  %s\n want %s", got, goldenPrivBytesHex)
	}

	pkixPub, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		t.Fatalf("MarshalPKIXPublicKey: %v", err)
	}
	if got := hex.EncodeToString(pkixPub); got != goldenPKIXPubHex {
		t.Fatalf("public point (X,Y) drifted from golden:\n got  %s\n want %s", got, goldenPKIXPubHex)
	}
}
