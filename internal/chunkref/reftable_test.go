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
func containsChunk(cands []GCCandidate, want ChunkID) bool {
	for _, c := range cands {
		if c.ChunkID == want {
			return true
		}
	}
	return false
}

func TestRemoveRefAbsentManifestOnSharedChunk(t *testing.T) {
	tbl := NewRefTable()
	m1 := ManifestID{Domain: DomainObjectVersion, VersionID: "v1"}
	m2 := ManifestID{Domain: DomainObjectVersion, VersionID: "v2"}
	c := ChunkID("chunk-1")
	now := time.Unix(1000, 0)

	tbl.AddRef(m1, c)         // set exists for c
	tbl.RemoveRef(m2, c, now) // m2 never added: set != nil but (m2, c) absent → no-op

	if got := tbl.RefCount(c); got != 1 {
		t.Fatalf("RefCount = %d, want 1 (no-op remove of absent manifest)", got)
	}
	if len(tbl.tombstones) != 0 {
		t.Fatalf("tombstone created for no-op RemoveRef, want none")
	}
}

func TestGCCandidatesExactBoundaryNotIncluded(t *testing.T) {
	tbl := NewRefTable()
	m := ManifestID{Domain: DomainObjectVersion, VersionID: "v1"}
	c := ChunkID("chunk-boundary")
	tZero := time.Unix(1000, 0)
	window := time.Hour

	tbl.AddRef(m, c)
	tbl.RemoveRef(m, c, tZero)

	// now - tZero == window: strict > is false, must NOT be a candidate.
	if cands := tbl.GCCandidates(tZero.Add(window), window); containsChunk(cands, c) {
		t.Fatalf("GCCandidates includes chunk at exact boundary, want excluded (strict >)")
	}
	// one nanosecond past the window: must be included.
	if cands := tbl.GCCandidates(tZero.Add(window+1), window); !containsChunk(cands, c) {
		t.Fatalf("GCCandidates missing chunk one ns past boundary, want included")
	}
}

func TestGCCandidatesPartialWindow(t *testing.T) {
	tbl := NewRefTable()
	m := ManifestID{Domain: DomainObjectVersion, VersionID: "v1"}
	old := ChunkID("chunk-old")
	recent := ChunkID("chunk-recent")
	base := time.Unix(0, 0)

	tbl.AddRef(m, old)
	tbl.RemoveRef(m, old, base) // t_zero = base (2h before now)
	tbl.AddRef(m, recent)
	tbl.RemoveRef(m, recent, base.Add(90*time.Minute)) // t_zero = 30min before now

	cands := tbl.GCCandidates(base.Add(2*time.Hour), time.Hour)
	if !containsChunk(cands, old) {
		t.Fatalf("old chunk missing from GCCandidates, want present")
	}
	if containsChunk(cands, recent) {
		t.Fatalf("recent chunk in GCCandidates, want excluded (within window)")
	}
}

func TestRebuildDeduplicatesIntraManifestChunks(t *testing.T) {
	m := ManifestID{Domain: DomainObjectVersion, VersionID: "v1"}
	c := ChunkID("chunk-dup")
	// Same chunk listed twice within one manifest must still yield refcount 1.
	rebuilt := Rebuild([]Manifest{
		{ID: m, Chunks: []ChunkID{c, c}},
	})
	if got := rebuilt.RefCount(c); got != 1 {
		t.Fatalf("RefCount = %d, want 1 (intra-manifest idempotent)", got)
	}
}
