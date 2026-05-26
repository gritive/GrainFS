package scrubber

import (
	"errors"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
)

type fakeManifestSource struct {
	liveVersions   []storage.SnapshotObject
	snapshotChunks []string
	listErr        error
	snapErr        error
}

func (f *fakeManifestSource) ListAllObjects() ([]storage.SnapshotObject, error) {
	return f.liveVersions, f.listErr
}
func (f *fakeManifestSource) SnapshotFrozenSegmentPaths(bucket string) ([]string, error) {
	return f.snapshotChunks, f.snapErr
}

func TestBuildKnownSegmentsIncludesAllVersionsAndSnapshots(t *testing.T) {
	src := &fakeManifestSource{
		liveVersions: []storage.SnapshotObject{
			{Bucket: "bkt", Key: "live", Segments: []storage.SegmentRef{{BlobID: "chunk-live"}}},
		},
		snapshotChunks: []string{"bkt/snap_segments/chunk-snap"},
	}
	known, err := buildKnownSegments("bkt", src)
	if err != nil {
		t.Fatalf("buildKnownSegments: %v", err)
	}
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
	known, err := buildKnownSegments("bkt", src)
	if err != nil {
		t.Fatalf("buildKnownSegments: %v", err)
	}
	if known["other/x_segments/c"] {
		t.Fatalf("must not include other bucket's segment")
	}
}

func TestBuildKnownSegmentsFailsClosedOnSourceError(t *testing.T) {
	// A source error must surface (caller skips the sweep) — never a partial set
	// that could mark a live chunk orphaned.
	listFail := &fakeManifestSource{listErr: errors.New("boom")}
	if _, err := buildKnownSegments("bkt", listFail); err == nil {
		t.Fatalf("expected error when ListAllObjects fails")
	}
	snapFail := &fakeManifestSource{snapErr: errors.New("boom")}
	if _, err := buildKnownSegments("bkt", snapFail); err == nil {
		t.Fatalf("expected error when SnapshotFrozenSegmentPaths fails")
	}
}
