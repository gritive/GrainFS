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

func tlsCertStub() tls.Certificate {
	cert, _, err := GenerateNodeIdentity("cluster-stub", "node-stub")
	if err != nil {
		panic(err)
	}
	return cert
}
