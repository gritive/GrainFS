package transport

import (
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

func TestNodeIDFromCert_RejectsMissingSAN(t *testing.T) {
	cert, _, err := DeriveClusterIdentity(longPSK("x"))
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if _, _, err := NodeIDFromCert(cert.Leaf); err == nil {
		t.Fatal("expected error for cert without grainfs SAN, got nil")
	}
}
