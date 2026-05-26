package snapshot

import (
	"testing"

	"github.com/gritive/GrainFS/internal/chunkref"
	"github.com/gritive/GrainFS/internal/storage"
)

func TestManifestsFromSnapshotsPinsFrozenChunks(t *testing.T) {
	snaps := []*Snapshot{
		{Seq: 7, Objects: []storage.SnapshotObject{
			{Bucket: "bkt", Key: "k", VersionID: "v1", Segments: []storage.SegmentRef{{BlobID: "c-frozen"}}},
		}},
	}
	tbl := chunkref.Rebuild(ManifestsFromSnapshots(snaps))
	if got := tbl.RefCount("c-frozen"); got != 1 {
		t.Fatalf("RefCount(c-frozen) = %d, want 1 (pinned by snapshot 7)", got)
	}
	if !tbl.Has(chunkref.SnapshotID(7), "c-frozen") {
		t.Fatalf("expected SnapshotID(7) manifest to reference c-frozen")
	}
}
