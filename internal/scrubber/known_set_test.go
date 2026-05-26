package scrubber

import (
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
)

type fakeManifestSource struct {
	liveVersions   []storage.SnapshotObject
	snapshotChunks []string
}

func (f *fakeManifestSource) ListAllObjects() ([]storage.SnapshotObject, error) {
	return f.liveVersions, nil
}
func (f *fakeManifestSource) SnapshotFrozenSegmentPaths(bucket string) ([]string, error) {
	return f.snapshotChunks, nil
}

func TestBuildKnownSegmentsIncludesAllVersionsAndSnapshots(t *testing.T) {
	src := &fakeManifestSource{
		liveVersions: []storage.SnapshotObject{
			{Bucket: "bkt", Key: "live", Segments: []storage.SegmentRef{{BlobID: "chunk-live"}}},
		},
		snapshotChunks: []string{"bkt/snap_segments/chunk-snap"},
	}
	known := buildKnownSegments("bkt", src)
	if !known["bkt/live_segments/chunk-live"] {
		t.Fatalf("live-version chunk missing from known set: %v", known)
	}
	if !known["bkt/snap_segments/chunk-snap"] {
		t.Fatalf("snapshot-frozen chunk missing from known set: %v", known)
	}
}

func TestBuildKnownSegmentsFiltersByBucket(t *testing.T) {
	src := &fakeManifestSource{
		liveVersions: []storage.SnapshotObject{
			{Bucket: "other", Key: "x", Segments: []storage.SegmentRef{{BlobID: "c"}}},
		},
	}
	known := buildKnownSegments("bkt", src)
	if known["other/x_segments/c"] {
		t.Fatalf("must not include other bucket's segment")
	}
}
