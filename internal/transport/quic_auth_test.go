package transport

import (
	"bytes"
	"crypto/sha256"
	"crypto/x509"
	"strings"
	"testing"
)

func TestPSKAlpn_Static(t *testing.T) {
	t1 := &QUICTransport{psk: "psk-A-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	t2 := &QUICTransport{psk: "psk-B-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}

	if got := t1.pskALPN(); got != "grainfs" {
		t.Fatalf("pskALPN: want %q, got %q", "grainfs", got)
	}
	if got := t1.muxALPN(); got != "grainfs-mux-v1" {
		t.Fatalf("muxALPN: want %q, got %q", "grainfs-mux-v1", got)
	}
	// PSK no longer affects ALPN.
	if t1.pskALPN() != t2.pskALPN() || t1.muxALPN() != t2.muxALPN() {
		t.Fatalf("ALPN must be static across PSKs after T2")
	}
}

func TestDeriveClusterIdentity_SPKIDeterministic(t *testing.T) {
	psk := strings.Repeat("a", 64)

	cert1, spki1, err := deriveClusterIdentity(psk)
	if err != nil {
		t.Fatalf("first derive: %v", err)
	}
	cert2, spki2, err := deriveClusterIdentity(psk)
	if err != nil {
		t.Fatalf("second derive: %v", err)
	}

	// SPKI must be stable for the same PSK (T1: keypair is deterministic).
	if spki1 != spki2 {
		t.Fatalf("SPKI not deterministic: %x vs %x", spki1, spki2)
	}

	// Cert DER must NOT be byte-identical (T1: signature uses crypto/rand).
	if bytes.Equal(cert1.Certificate[0], cert2.Certificate[0]) {
		t.Fatalf("cert DER byte-identical — signature randomness missing")
	}

	// Sanity: SPKI from parsed cert matches the returned spki.
	parsed, err := x509.ParseCertificate(cert1.Certificate[0])
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}
	got := sha256.Sum256(parsed.RawSubjectPublicKeyInfo)
	if got != spki1 {
		t.Fatalf("returned SPKI does not match cert SPKI")
	}
}

func TestDeriveClusterIdentity_DifferentPSKDifferentSPKI(t *testing.T) {
	pskA := strings.Repeat("a", 64)
	pskB := strings.Repeat("b", 64)

	_, spkiA, err := deriveClusterIdentity(pskA)
	if err != nil {
		t.Fatal(err)
	}
	_, spkiB, err := deriveClusterIdentity(pskB)
	if err != nil {
		t.Fatal(err)
	}

	if spkiA == spkiB {
		t.Fatalf("different PSKs produced same SPKI")
	}
}

func TestDeriveClusterIdentity_RejectsEmpty(t *testing.T) {
	_, _, err := deriveClusterIdentity("")
	if err == nil || err != ErrEmptyClusterKey {
		t.Fatalf("want ErrEmptyClusterKey, got %v", err)
	}
}

func TestNewQUICTransport_RejectsEmptyPSK(t *testing.T) {
	_, err := NewQUICTransport("")
	if err != ErrEmptyClusterKey {
		t.Fatalf("want ErrEmptyClusterKey, got %v", err)
	}
}

func TestNewQUICTransport_AcceptsShortPSK(t *testing.T) {
	tr, err := NewQUICTransport("short")
	if err != nil {
		t.Fatalf("short PSK should not block construction: %v", err)
	}
	if tr == nil {
		t.Fatal("nil transport for short PSK")
	}
}

func TestNewQUICTransport_CachesSPKI(t *testing.T) {
	psk := strings.Repeat("a", 64)
	tr, err := NewQUICTransport(psk)
	if err != nil {
		t.Fatal(err)
	}
	spki1 := tr.expectedSPKI
	spki2 := tr.expectedSPKI
	if spki1 != spki2 {
		t.Fatal("expectedSPKI mutated unexpectedly")
	}
	_, want, _ := deriveClusterIdentity(psk)
	if spki1 != want {
		t.Fatalf("cached SPKI %x != fresh derive %x", spki1, want)
	}
}
