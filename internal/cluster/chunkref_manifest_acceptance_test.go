package cluster

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/chunkref"
)

// Acceptance gate: an object's chunk references are recoverable from the
// data-group live-version manifest (ListAllObjectsStrict) ALONE — no meta-Raft
// refcount index needed.
func TestChunkRefsRebuildableFromManifestAlone(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	if err := b.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatalf("bucket: %v", err)
	}
	seedNonVersionedObjectWithSegments(t, b, "bkt", "k1",
		[]SegmentMetaEntry{{BlobID: "c-A", SegmentIdx: 0}, {BlobID: "c-B", SegmentIdx: 1}}, nil)
	seedNonVersionedObjectWithSegments(t, b, "bkt", "k2",
		[]SegmentMetaEntry{{BlobID: "c-A", SegmentIdx: 0}}, nil)

	objs, err := b.ListAllObjectsStrict()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	tbl := chunkref.Rebuild(ManifestsFromSnapshotObjects(objs))
	if got := tbl.RefCount("c-A"); got != 2 {
		t.Fatalf("RefCount(c-A)=%d want 2 (shared by k1,k2)", got)
	}
	if got := tbl.RefCount("c-B"); got != 1 {
		t.Fatalf("RefCount(c-B)=%d want 1", got)
	}
}
