package transport

import (
	"crypto/ecdsa"
	"net/url"
	"testing"
)

func TestGenerateNodeIdentity_UniqueAndSAN(t *testing.T) {
	clusterID := "11111111-2222-3333-4444-555555555555"
	c1, spki1, err := GenerateNodeIdentity(clusterID, "node-a")
	if err != nil {
		t.Fatalf("gen node-a: %v", err)
	}
	_, spki2, err := GenerateNodeIdentity(clusterID, "node-b")
	if err != nil {
		t.Fatalf("gen node-b: %v", err)
	}
	if spki1 == spki2 {
		t.Fatal("two nodes produced identical SPKI; keys are not unique/random")
	}
	_, spki1b, err := GenerateNodeIdentity(clusterID, "node-a")
	if err != nil {
		t.Fatalf("regen node-a: %v", err)
	}
	if spki1 == spki1b {
		t.Fatal("regenerating node-a produced the same SPKI; key is deterministic, expected random")
	}
	leaf := c1.Leaf
	if leaf == nil {
		t.Fatal("cert Leaf is nil")
	}
	if len(leaf.URIs) != 1 {
		t.Fatalf("want 1 SAN URI, got %d", len(leaf.URIs))
	}
	want := &url.URL{Scheme: "grainfs", Host: clusterID, Path: "/node-a"}
	if leaf.URIs[0].String() != want.String() {
		t.Fatalf("SAN URI = %q, want %q", leaf.URIs[0].String(), want.String())
	}
	gotCluster, gotNode, err := NodeIDFromCert(leaf)
	if err != nil {
		t.Fatalf("NodeIDFromCert: %v", err)
	}
	if gotCluster != clusterID || gotNode != "node-a" {
		t.Fatalf("NodeIDFromCert = (%q,%q), want (%q,node-a)", gotCluster, gotNode, clusterID)
	}
}

// TestBuildNodeIdentity_ReusesKeySameSPKI verifies the invite-join resume path's
// identity reuse: rebuilding a cert from an EXISTING key yields the SAME SPKI as
// the original (so a Phase-1 retry presents the SPKI the leader already bound).
func TestBuildNodeIdentity_ReusesKeySameSPKI(t *testing.T) {
	clusterID := "11111111-2222-3333-4444-555555555555"
	c1, spki1, err := GenerateNodeIdentity(clusterID, "node-a")
	if err != nil {
		t.Fatalf("gen: %v", err)
	}
	priv, ok := c1.PrivateKey.(*ecdsa.PrivateKey)
	if !ok {
		t.Fatalf("PrivateKey is %T, want *ecdsa.PrivateKey", c1.PrivateKey)
	}
	_, spki2, err := BuildNodeIdentity(clusterID, "node-a", priv)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if spki1 != spki2 {
		t.Fatalf("rebuilt SPKI %x != original %x; reused key must yield same SPKI", spki2, spki1)
	}
}

func TestNodeIDFromCert_RejectsMissingSAN(t *testing.T) {
	cert, _, err := DeriveClusterIdentity(longPSK("x"))
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if _, _, err := NodeIDFromCert(cert.Leaf); err == nil {
		t.Fatal("expected error for cert without grainfs SAN, got nil")
	}
}
