package chunkref

import (
	"testing"
	"time"
)

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

func TestRemoveRefIdempotent(t *testing.T) {
	tbl := NewRefTable()
	m := ManifestID{Domain: DomainObjectVersion, VersionID: "bkt/obj/v1"}
	c := ChunkID("chunk-1")
	now := time.Unix(1000, 0)

	tbl.AddRef(m, c)
	tbl.RemoveRef(m, c, now)
	tbl.RemoveRef(m, c, now) // removing absent (m, c) is a no-op

	if got := tbl.RefCount(c); got != 0 {
		t.Fatalf("RefCount = %d, want 0", got)
	}
	if tbl.Has(m, c) {
		t.Fatalf("Has(m, c) = true after remove, want false")
	}
}

func TestRemoveRefToZeroCreatesTombstone(t *testing.T) {
	tbl := NewRefTable()
	m := ManifestID{Domain: DomainObjectVersion, VersionID: "bkt/obj/v1"}
	c := ChunkID("chunk-1")
	tZero := time.Unix(1000, 0)

	tbl.AddRef(m, c)
	tbl.RemoveRef(m, c, tZero)

	cands := tbl.GCCandidates(tZero.Add(2*time.Hour), time.Hour)
	if !containsChunk(cands, c) {
		t.Fatalf("GCCandidates = %v, want to contain %q", cands, c)
	}
}

func TestGCCandidatesRespectsWindow(t *testing.T) {
	tbl := NewRefTable()
	m := ManifestID{Domain: DomainObjectVersion, VersionID: "bkt/obj/v1"}
	c := ChunkID("chunk-1")
	tZero := time.Unix(1000, 0)

	tbl.AddRef(m, c)
	tbl.RemoveRef(m, c, tZero)

	cands := tbl.GCCandidates(tZero.Add(30*time.Minute), time.Hour)
	if containsChunk(cands, c) {
		t.Fatalf("GCCandidates = %v, want empty before window elapses", cands)
	}
}

func TestReAddEvictsTombstone(t *testing.T) {
	tbl := NewRefTable()
	m1 := ManifestID{Domain: DomainObjectVersion, VersionID: "bkt/obj/v1"}
	m2 := ManifestID{Domain: DomainSnapshot, VersionID: "snap-1"}
	c := ChunkID("chunk-1")
	tZero := time.Unix(1000, 0)

	tbl.AddRef(m1, c)
	tbl.RemoveRef(m1, c, tZero) // ref → 0, tombstone created
	tbl.AddRef(m2, c)           // referenced again → tombstone evicted

	cands := tbl.GCCandidates(tZero.Add(2*time.Hour), time.Hour)
	if containsChunk(cands, c) {
		t.Fatalf("GCCandidates = %v, want empty (re-referenced chunk evicts tombstone)", cands)
	}
	if got := tbl.RefCount(c); got != 1 {
		t.Fatalf("RefCount = %d, want 1", got)
	}
}

// containsChunk is an order-independent membership helper (GCCandidates order is
// unspecified because it iterates a map).
func containsChunk(chunks []ChunkID, want ChunkID) bool {
	for _, c := range chunks {
		if c == want {
			return true
		}
	}
	return false
}
