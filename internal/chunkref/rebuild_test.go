package chunkref

import (
	"reflect"
	"testing"
)

func TestRebuildEquivalentToSequentialAddRef(t *testing.T) {
	m1 := ManifestID{Domain: DomainObjectVersion, VersionID: "bkt/a/v1"}
	m2 := ManifestID{Domain: DomainSnapshot, VersionID: "snap-1"}
	cA := ChunkID("chunk-a")
	cB := ChunkID("cas://b3-b")

	manifests := []Manifest{
		{ID: m1, Chunks: []ChunkID{cA, cB}},
		{ID: m2, Chunks: []ChunkID{cA}},
	}
	rebuilt := Rebuild(manifests)

	seq := NewRefTable()
	seq.AddRef(m1, cA)
	seq.AddRef(m1, cB)
	seq.AddRef(m2, cA)

	if !reflect.DeepEqual(rebuilt.refs, seq.refs) {
		t.Fatalf("rebuilt.refs = %v, want %v", rebuilt.refs, seq.refs)
	}
	if rebuilt.RefCount(cA) != 2 {
		t.Fatalf("RefCount(cA) = %d, want 2", rebuilt.RefCount(cA))
	}
	if rebuilt.RefCount(cB) != 1 {
		t.Fatalf("RefCount(cB) = %d, want 1", rebuilt.RefCount(cB))
	}
}

func TestRebuildHasNoTombstones(t *testing.T) {
	rebuilt := Rebuild([]Manifest{
		{ID: ManifestID{Domain: DomainObjectVersion, VersionID: "v1"}, Chunks: []ChunkID{"c1"}},
	})
	if len(rebuilt.tombstones) != 0 {
		t.Fatalf("rebuilt tombstones = %v, want none", rebuilt.tombstones)
	}
}

func TestRebuildEmpty(t *testing.T) {
	rebuilt := Rebuild(nil)
	if rebuilt.RefCount("anything") != 0 {
		t.Fatalf("empty rebuild has refs")
	}
}
