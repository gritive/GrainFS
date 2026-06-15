package snapshot

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestAllFrozenObjectVersions_EmptyDir(t *testing.T) {
	m := NewTestManager(t, t.TempDir(), nil, "")
	got, err := m.AllFrozenObjectVersions()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty dir must yield no refs, got %v", got)
	}
}

func TestAllFrozenObjectVersions_FiltersMarkersAndUnversioned(t *testing.T) {
	dir := t.TempDir()
	m := NewTestManager(t, dir, nil, "")
	snap := &Snapshot{
		Seq:     1,
		Buckets: []string{"b1"},
		Objects: []storage.SnapshotObject{
			{Bucket: "b1", Key: "live", VersionID: "v1"},                          // keep
			{Bucket: "b1", Key: "dir/obj", VersionID: "v2"},                       // keep (slash key)
			{Bucket: "b1", Key: "deleted", VersionID: "v3", IsDeleteMarker: true}, // drop (marker)
			{Bucket: "b1", Key: "nover", VersionID: ""},                           // drop (no version)
		},
	}
	if err := m.writeSnapshot(m.path(1), snap); err != nil {
		t.Fatal(err)
	}
	got, err := m.AllFrozenObjectVersions()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"b1/live/v1": true, "b1/dir/obj/v2": true}
	if len(got) != len(want) {
		t.Fatalf("got %d refs %v, want %d", len(got), got, len(want))
	}
	for _, r := range got {
		k := r.Bucket + "/" + r.Key + "/" + r.VersionID
		if !want[k] {
			t.Fatalf("unexpected ref %q", k)
		}
	}
}

func TestAllFrozenObjectVersions_CorruptDescriptorFailsClosed(t *testing.T) {
	dir := t.TempDir()
	m := NewTestManager(t, dir, nil, "")
	if err := os.WriteFile(filepath.Join(dir, "snapshot-7.json.zst"), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m.AllFrozenObjectVersions(); err == nil {
		t.Fatal("corrupt descriptor must cause AllFrozenObjectVersions to error (fail-closed)")
	}
}

func TestAllFrozenObjectVersions_DedupAcrossSnapshots(t *testing.T) {
	dir := t.TempDir()
	m := NewTestManager(t, dir, nil, "")
	obj := storage.SnapshotObject{Bucket: "b1", Key: "k", VersionID: "v1"}
	if err := m.writeSnapshot(m.path(1), &Snapshot{Seq: 1, Buckets: []string{"b1"}, Objects: []storage.SnapshotObject{obj}}); err != nil {
		t.Fatal(err)
	}
	if err := m.writeSnapshot(m.path(2), &Snapshot{Seq: 2, Buckets: []string{"b1"}, Objects: []storage.SnapshotObject{obj}}); err != nil {
		t.Fatal(err)
	}
	got, err := m.AllFrozenObjectVersions()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("dedup across snapshots: got %d refs, want 1: %v", len(got), got)
	}
}
