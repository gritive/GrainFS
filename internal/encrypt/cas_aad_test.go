package encrypt

import (
	"bytes"
	"testing"
)

func TestCASChunkAAD_ObjectIndependent(t *testing.T) {
	clusterID := bytes.Repeat([]byte{0xAB}, 16)
	locator := []byte("blake3-digest-of-plaintext")

	// Two callers standing in for two different objects (bucket/key) that
	// reference the SAME canonical chunk must derive byte-identical AAD.
	a := CASChunkAAD(clusterID, locator)
	b := CASChunkAAD(clusterID, locator)
	if !bytes.Equal(a, b) {
		t.Fatalf("CAS AAD must be object-independent: %x != %x", a, b)
	}
}

func TestCASChunkAAD_DistinctLocatorsDiffer(t *testing.T) {
	clusterID := bytes.Repeat([]byte{0xAB}, 16)
	a := CASChunkAAD(clusterID, []byte("locator-A"))
	b := CASChunkAAD(clusterID, []byte("locator-B"))
	if bytes.Equal(a, b) {
		t.Fatal("different canonical locators must produce different AAD")
	}
}

func TestCASChunkAAD_UsesCASDomain(t *testing.T) {
	clusterID := bytes.Repeat([]byte{0}, 16)
	locator := []byte("x")
	got := CASChunkAAD(clusterID, locator)
	want := BuildAAD(DomainCASChunk, clusterID, FieldBytes(locator))
	if !bytes.Equal(got, want) {
		t.Fatalf("CASChunkAAD must equal BuildAAD(DomainCASChunk,...): %x != %x", got, want)
	}
	// Domain separation: a shard-domain AAD over the same bytes must differ,
	// so a legacy shard ciphertext can never verify as a CAS chunk.
	shard := BuildAAD(DomainShard, clusterID, FieldBytes(locator))
	if bytes.Equal(got, shard) {
		t.Fatal("CAS and shard domains must not collide for identical fields")
	}
}

func TestCASChunkAAD_PanicsOnBadClusterID(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for cluster_id len != 16")
		}
	}()
	CASChunkAAD([]byte{0x01}, []byte("x"))
}
