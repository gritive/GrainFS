package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/chunkref"
	"github.com/gritive/GrainFS/internal/storage"
)

func TestManifestsFromSnapshotObjects(t *testing.T) {
	objs := []storage.SnapshotObject{
		{Bucket: "bkt", Key: "k1", VersionID: "v1", Segments: []storage.SegmentRef{{BlobID: "c-A"}, {BlobID: "c-B"}}},
		{Bucket: "bkt", Key: "k2", VersionID: "v1", Segments: []storage.SegmentRef{{BlobID: "c-A"}}},
	}
	tbl := chunkref.Rebuild(ManifestsFromSnapshotObjects(objs))
	if got := tbl.RefCount("c-A"); got != 2 {
		t.Fatalf("RefCount(c-A) = %d, want 2 (shared by k1,k2)", got)
	}
	if got := tbl.RefCount("c-B"); got != 1 {
		t.Fatalf("RefCount(c-B) = %d, want 1", got)
	}
	m1 := chunkref.ObjectVersionID("bkt", "k1", "v1")
	if !tbl.Has(m1, "c-A") {
		t.Fatalf("expected k1 manifest to reference c-A")
	}
}
