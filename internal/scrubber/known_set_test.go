package scrubber

import (
	"context"
	"errors"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
)

// stubScrubbable is a no-op Scrubbable so New() can be constructed inside
// package scrubber for hoist tests (the mock in scrubber_test.go lives in the
// external scrubber_test package and is unreachable here).
type stubScrubbable struct{}

func (stubScrubbable) ListBuckets(context.Context) ([]string, error)    { return nil, nil }
func (stubScrubbable) ScanObjects(string) (<-chan ObjectRecord, error)  { return nil, nil }
func (stubScrubbable) ObjectExists(string, string) (bool, error)        { return true, nil }
func (stubScrubbable) ShardPaths(string, string, string, int) []string  { return nil }
func (stubScrubbable) ReadShard(string, string, string) ([]byte, error) { return nil, nil }
func (stubScrubbable) WriteShard(string, string, string, []byte) error  { return nil }

// fakeManifestSource implements Scrubbable (via stubScrubbable) plus
// segmentManifestSource so it can be passed to New() for hoist tests.
type fakeManifestSource struct {
	stubScrubbable
	liveVersions []storage.SnapshotObject
	frozen       map[string][]string
	listErr      error
	frozenErr    error
}

func newFakeManifestSource() *fakeManifestSource {
	return &fakeManifestSource{}
}

func (f *fakeManifestSource) ListAllObjects() ([]storage.SnapshotObject, error) {
	return f.liveVersions, f.listErr
}
func (f *fakeManifestSource) AllFrozenSegmentPaths() (map[string][]string, error) {
	return f.frozen, f.frozenErr
}

func TestBuildKnownSegmentsMergesLiveAndFrozen(t *testing.T) {
	segByBucket := map[string]map[string]bool{
		"bkt": {"bkt/live_segments/chunk-live": true},
	}
	frozenByBucket := map[string][]string{
		"bkt": {"bkt/snap_segments/chunk-snap"},
	}
	known := buildKnownSegments("bkt", segByBucket, frozenByBucket)
	if !known["bkt/live_segments/chunk-live"] {
		t.Fatalf("live-version chunk missing from known set: %v", known)
	}
	if !known["bkt/snap_segments/chunk-snap"] {
		t.Fatalf("snapshot-frozen chunk missing from known set: %v", known)
	}
}

func TestBuildKnownSegmentsScopedToBucket(t *testing.T) {
	segByBucket := map[string]map[string]bool{
		"other": {"other/x_segments/c": true},
	}
	known := buildKnownSegments("bkt", segByBucket, nil)
	if known["other/x_segments/c"] {
		t.Fatalf("must not include other bucket's segment")
	}
	if len(known) != 0 {
		t.Fatalf("expected empty known set for bkt, got %v", known)
	}
}

func TestHoistSegmentSourcesBuildsGroupedSets(t *testing.T) {
	src := newFakeManifestSource()
	src.liveVersions = []storage.SnapshotObject{
		{Bucket: "bkt", Key: "live", Segments: []storage.SegmentRef{{BlobID: "chunk-live"}}},
	}
	src.frozen = map[string][]string{"bkt": {"bkt/snap_segments/chunk-snap"}}
	s := New(src, 0)
	segByBucket, frozenByBucket, ok := s.hoistSegmentSources()
	if !ok {
		t.Fatalf("hoist should succeed")
	}
	if !segByBucket["bkt"]["bkt/live_segments/chunk-live"] {
		t.Fatalf("live segment missing: %v", segByBucket)
	}
	if frozenByBucket["bkt"][0] != "bkt/snap_segments/chunk-snap" {
		t.Fatalf("frozen path missing: %v", frozenByBucket)
	}
}

func TestHoistSegmentSourcesFailsClosedOnSourceError(t *testing.T) {
	// A source error must yield ok=false (caller skips the whole sweep) — never a
	// partial set that could mark a live chunk orphaned.
	listFail := newFakeManifestSource()
	listFail.listErr = errors.New("boom")
	if _, _, ok := New(listFail, 0).hoistSegmentSources(); ok {
		t.Fatalf("expected ok=false when ListAllObjects fails")
	}
	frozenFail := newFakeManifestSource()
	frozenFail.frozenErr = errors.New("boom")
	if _, _, ok := New(frozenFail, 0).hoistSegmentSources(); ok {
		t.Fatalf("expected ok=false when AllFrozenSegmentPaths fails")
	}
}

func TestHoistSegmentSourcesNoManifestSource(t *testing.T) {
	// A backend that does not implement segmentManifestSource yields
	// (nil, nil, true): the per-bucket loop runs against appendables only.
	s := New(stubScrubbable{}, 0)
	segByBucket, frozenByBucket, ok := s.hoistSegmentSources()
	if !ok || segByBucket != nil || frozenByBucket != nil {
		t.Fatalf("expected (nil, nil, true) for non-manifest backend, got (%v, %v, %v)", segByBucket, frozenByBucket, ok)
	}
}
