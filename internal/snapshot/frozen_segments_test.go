package snapshot

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestAllFrozenSegmentPaths_EmptyDir verifies that an empty snapshot directory
// returns a non-nil map (safe for callers to range over) and no error.
func TestAllFrozenSegmentPaths_EmptyDir(t *testing.T) {
	m, err := NewManager(t.TempDir(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	got, err := m.AllFrozenSegmentPaths()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("AllFrozenSegmentPaths must return a non-nil map for an empty dir")
	}
}

func TestAllFrozenSegmentPaths_PathFormAndGrouping(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	seg := storage.SegmentRef{BlobID: "01HXYZ-raw-segment"}
	snap := &Snapshot{
		Seq:     1,
		Buckets: []string{"b1"},
		Objects: []storage.SnapshotObject{{Bucket: "b1", Key: "dir/obj", VersionID: "v1", Segments: []storage.SegmentRef{seg}}},
	}
	if err := writeSnapshot(m.path(1), snap); err != nil {
		t.Fatal(err)
	}

	got, err := m.AllFrozenSegmentPaths()
	if err != nil {
		t.Fatal(err)
	}
	want := storage.SegmentKnownPath("b1", "dir/obj", seg.BlobID)
	if len(got["b1"]) != 1 || got["b1"][0] != want {
		t.Fatalf("got %v, want b1=[%s]", got, want)
	}
}

func TestAllFrozenSegmentPaths_CorruptDescriptorFailsClosed(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	// write a file matching the descriptor name pattern but with garbage content
	if err := os.WriteFile(filepath.Join(dir, "snapshot-7.json.zst"), []byte("not-a-valid-zst-descriptor"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m.AllFrozenSegmentPaths(); err == nil {
		t.Fatal("corrupt descriptor must cause AllFrozenSegmentPaths to error (fail-closed)")
	}
}

func TestAllFrozenSegmentPaths_DedupAcrossSnapshots(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	seg := storage.SegmentRef{BlobID: "shared-blob"}
	obj := storage.SnapshotObject{Bucket: "b1", Key: "k", VersionID: "v1", Segments: []storage.SegmentRef{seg}}
	if err := writeSnapshot(m.path(1), &Snapshot{Seq: 1, Buckets: []string{"b1"}, Objects: []storage.SnapshotObject{obj}}); err != nil {
		t.Fatal(err)
	}
	if err := writeSnapshot(m.path(2), &Snapshot{Seq: 2, Buckets: []string{"b1"}, Objects: []storage.SnapshotObject{obj}}); err != nil {
		t.Fatal(err)
	}
	got, err := m.AllFrozenSegmentPaths()
	if err != nil {
		t.Fatal(err)
	}
	if len(got["b1"]) != 1 {
		t.Fatalf("same segment in 2 snapshots must dedup, got %v", got["b1"])
	}
}
