package cluster

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/chunkref"
	"github.com/gritive/GrainFS/internal/snapshot"
	"github.com/gritive/GrainFS/internal/storage"
)

// Spec acceptance gate: a snapshot's chunk references are recoverable from the
// data-group manifest (ListAllObjects) ALONE — no meta-Raft refcount index needed.
//
// Package placement: cluster_test.go imports snapshot only in test files.
// snapshot does not import cluster, so no import cycle.
func TestSnapshotChunkRefsRebuildableFromManifestAlone(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	if err := b.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatalf("bucket: %v", err)
	}
	seedObjectWithSegments(t, b, "bkt", "k1", "v1",
		[]storage.SegmentRef{{BlobID: "c-A"}, {BlobID: "c-B"}}, nil)
	seedObjectWithSegments(t, b, "bkt", "k2", "v1",
		[]storage.SegmentRef{{BlobID: "c-A"}}, nil)

	objs, err := b.ListAllObjects()
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

// Data-loss gate: a chunk frozen by a snapshot stays referenced in the rebuilt
// table after the live object is deleted (live-versions + snapshot-manifests concat).
//
// Delete approach: excludeKey is used to simulate the live object being gone from
// the live set. The cluster DeleteObject API creates a delete marker (tombstone
// semantics), not a hard delete — the seeded objectMeta written directly via
// badger would still appear in ListAllObjects after a soft delete. excludeKey
// cleanly proves the property without coupling to the apply/propose path.
func TestSnapshotPinSurvivesLiveDelete(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	if err := b.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatalf("bucket: %v", err)
	}
	seedObjectWithSegments(t, b, "bkt", "k", "v1",
		[]storage.SegmentRef{{BlobID: "c-frozen"}}, nil)

	mgr, err := snapshot.NewManagerWithEncryptor(t.TempDir(), b, "", nil)
	if err != nil {
		t.Fatalf("snap mgr: %v", err)
	}
	snap, err := mgr.Create("gate")
	if err != nil {
		t.Fatalf("snapshot create: %v", err)
	}

	// Simulate the live object being gone by excluding it from the live set.
	// This proves the snapshot manifest alone keeps the chunk pinned.
	liveObjs, err := b.ListAllObjects()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	liveObjs = excludeKey(liveObjs, "k") // simulate deletion of k from the live set

	manifests := ManifestsFromSnapshotObjects(liveObjs)
	manifests = append(manifests, snapshot.ManifestsFromSnapshots([]*snapshot.Snapshot{snap})...)
	tbl := chunkref.Rebuild(manifests)
	if got := tbl.RefCount("c-frozen"); got != 1 {
		t.Fatalf("RefCount(c-frozen)=%d after live delete, want 1 (snapshot pins it)", got)
	}
}

func excludeKey(objs []storage.SnapshotObject, key string) []storage.SnapshotObject {
	out := objs[:0:0]
	for _, o := range objs {
		if o.Key != key {
			out = append(out, o)
		}
	}
	return out
}
