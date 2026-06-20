package cluster

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/chunkref"
	"github.com/gritive/GrainFS/internal/storage"
)

// Acceptance gate: an object's chunk references are recoverable from the
// data-group live-version manifest (ListAllObjectsStrict) ALONE — no meta-Raft
// refcount index needed.
func TestChunkRefsRebuildableFromManifestAlone(t *testing.T) {
	b, db := newTestDistributedBackendWithDB(t)
	ctx := context.Background()
	if err := b.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatalf("bucket: %v", err)
	}
	seedObjectWithSegments(t, b, db, "bkt", "k1", "v1",
		[]storage.SegmentRef{{BlobID: "c-A"}, {BlobID: "c-B"}}, nil)
	seedObjectWithSegments(t, b, db, "bkt", "k2", "v1",
		[]storage.SegmentRef{{BlobID: "c-A"}}, nil)

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
