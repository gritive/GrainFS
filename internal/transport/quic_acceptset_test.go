package transport

import (
	"crypto/tls"
	"testing"
)

func TestIdentitySnapshot_AcceptSetLookup(t *testing.T) {
	var a, b, c [32]byte
	a[0], b[0], c[0] = 1, 2, 3
	snap := NewIdentitySnapshot([][32]byte{a, b}, tlsCertStub(), a)
	if !snap.Accepts(a) || !snap.Accepts(b) {
		t.Fatal("snapshot should accept its configured SPKIs")
	}
	if snap.Accepts(c) {
		t.Fatal("snapshot must not accept an unregistered SPKI")
	}
}

// A snapshot built via a raw struct literal has a nil acceptSet; Accepts must
// still verify correctly via the AcceptSPKIs fallback (not silently reject all).
func TestIdentitySnapshot_LiteralFallbackAccepts(t *testing.T) {
	var a, b [32]byte
	a[0], b[0] = 1, 2
	snap := &IdentitySnapshot{AcceptSPKIs: [][32]byte{a}}
	if !snap.Accepts(a) {
		t.Fatal("literal snapshot must accept a configured SPKI via fallback")
	}
	if snap.Accepts(b) {
		t.Fatal("literal snapshot must reject an unconfigured SPKI")
	}
}

func tlsCertStub() tls.Certificate {
	cert, _, err := GenerateNodeIdentity("cluster-stub", "node-stub")
	if err != nil {
		panic(err)
	}
	return cert
}
