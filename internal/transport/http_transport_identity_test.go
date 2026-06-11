package transport

import (
	"errors"
	"testing"
)

func TestHTTPTransport_EmptyPSK(t *testing.T) {
	if _, err := NewHTTPTransport(""); !errors.Is(err, ErrEmptyClusterKey) {
		t.Fatalf("NewHTTPTransport(\"\") err = %v, want ErrEmptyClusterKey", err)
	}
}

func TestHTTPTransport_SeedIdentity(t *testing.T) {
	_, baseSPKI, err := DeriveClusterIdentity("psk-a")
	if err != nil {
		t.Fatal(err)
	}
	tr := MustNewHTTPTransport("psk-a")
	snap := tr.identity.Load()
	if !snap.Accepts(baseSPKI) {
		t.Fatal("seed snapshot must accept the base PSK SPKI")
	}
	if snap.PresentSPKI != baseSPKI {
		t.Fatalf("PresentSPKI = %x, want base %x", snap.PresentSPKI, baseSPKI)
	}
}

func TestHTTPTransport_FlipPresent(t *testing.T) {
	cert2, spki2, err := DeriveClusterIdentity("psk-b")
	if err != nil {
		t.Fatal(err)
	}
	tr := MustNewHTTPTransport("psk-a")
	tr.FlipPresent(cert2, spki2)
	if got := tr.identity.Load().PresentSPKI; got != spki2 {
		t.Fatalf("after FlipPresent PresentSPKI = %x, want %x", got, spki2)
	}
}

func TestHTTPTransport_SetDroppedAndRegistry(t *testing.T) {
	_, baseSPKI, err := DeriveClusterIdentity("psk-a")
	if err != nil {
		t.Fatal(err)
	}
	_, peerSPKI, err := DeriveClusterIdentity("peer-node")
	if err != nil {
		t.Fatal(err)
	}
	tr := MustNewHTTPTransport("psk-a")

	tr.SetDropped()
	if tr.identity.Load().Accepts(baseSPKI) {
		t.Fatal("after SetDropped the base PSK SPKI must no longer be accepted")
	}

	tr.UpdateRegistryAccept([][32]byte{peerSPKI})
	if !tr.identity.Load().Accepts(peerSPKI) {
		t.Fatal("registry SPKI must be accepted after UpdateRegistryAccept")
	}
	if tr.identity.Load().Accepts(baseSPKI) {
		t.Fatal("base PSK SPKI must stay dropped after a registry update")
	}
}
