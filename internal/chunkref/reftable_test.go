package chunkref

import "testing"

func TestAddRefIdempotent(t *testing.T) {
	tbl := NewRefTable()
	m := ManifestID{Domain: DomainObjectVersion, VersionID: "bkt/obj/v1"}
	c := ChunkID("0192f3c0-aaaa-7bbb-8ccc-000000000001")

	tbl.AddRef(m, c)
	tbl.AddRef(m, c) // duplicate (m, c) is a no-op

	if got := tbl.RefCount(c); got != 1 {
		t.Fatalf("RefCount = %d, want 1 (set semantics)", got)
	}
	if !tbl.Has(m, c) {
		t.Fatalf("Has(m, c) = false, want true")
	}
}

func TestRefCountDistinctManifests(t *testing.T) {
	tbl := NewRefTable()
	c := ChunkID("cas://b3-deadbeef")
	m1 := ManifestID{Domain: DomainObjectVersion, VersionID: "bkt/a/v1"}
	m2 := ManifestID{Domain: DomainSnapshot, VersionID: "snap-1"}

	tbl.AddRef(m1, c)
	tbl.AddRef(m2, c)

	if got := tbl.RefCount(c); got != 2 {
		t.Fatalf("RefCount = %d, want 2 (distinct manifests)", got)
	}
}

func TestRefCountAbsentChunkIsZero(t *testing.T) {
	tbl := NewRefTable()
	if got := tbl.RefCount(ChunkID("never-added")); got != 0 {
		t.Fatalf("RefCount = %d, want 0", got)
	}
}
