package snapshot

import (
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/chunkref"
	"github.com/gritive/GrainFS/internal/storage"
)

// fakeRefSink records AddRef/RemoveRef calls for test assertions.
type fakeRefSink struct {
	added   map[chunkref.ManifestID][]chunkref.ChunkID
	removed map[chunkref.ManifestID][]chunkref.ChunkID
}

func newFakeRefSink() *fakeRefSink {
	return &fakeRefSink{
		added:   map[chunkref.ManifestID][]chunkref.ChunkID{},
		removed: map[chunkref.ManifestID][]chunkref.ChunkID{},
	}
}

func (f *fakeRefSink) AddRef(m chunkref.ManifestID, c chunkref.ChunkID) error {
	f.added[m] = append(f.added[m], c)
	return nil
}
func (f *fakeRefSink) RemoveRef(m chunkref.ManifestID, c chunkref.ChunkID, now time.Time) error {
	f.removed[m] = append(f.removed[m], c)
	return nil
}

// fakeSnapshotableWithObjects is a minimal storage.Snapshotable for chunkref tests.
type fakeSnapshotableWithObjects struct {
	objects []storage.SnapshotObject
}

func newFakeSnapshotableWithObjects(objs []storage.SnapshotObject) storage.Snapshotable {
	return &fakeSnapshotableWithObjects{objects: objs}
}

func (f *fakeSnapshotableWithObjects) ListAllObjects() ([]storage.SnapshotObject, error) {
	return f.objects, nil
}

func (f *fakeSnapshotableWithObjects) RestoreObjects(objects []storage.SnapshotObject) (int, []storage.StaleBlob, error) {
	f.objects = objects
	return len(objects), nil, nil
}

func TestCreatePinsChunksViaSnapshotManifest(t *testing.T) {
	sink := newFakeRefSink()
	backend := newFakeSnapshotableWithObjects([]storage.SnapshotObject{
		{Bucket: "bkt", Key: "k", Segments: []storage.SegmentRef{{BlobID: "chunk-A"}}},
	})
	m, err := NewManagerWithRefSink(t.TempDir(), backend, "", nil, sink)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	snap, err := m.Create("test")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	mid := chunkref.SnapshotID(snap.Seq)
	if got := sink.added[mid]; len(got) != 1 || got[0] != "chunk-A" {
		t.Fatalf("AddRef for snapshot manifest = %v, want [chunk-A]", got)
	}
}

func TestDeleteUnpinsChunks(t *testing.T) {
	sink := newFakeRefSink()
	backend := newFakeSnapshotableWithObjects([]storage.SnapshotObject{
		{Bucket: "bkt", Key: "k", Segments: []storage.SegmentRef{{BlobID: "chunk-A"}}},
	})
	m, _ := NewManagerWithRefSink(t.TempDir(), backend, "", nil, sink)
	snap, _ := m.Create("test")
	if err := m.Delete(snap.Seq); err != nil {
		t.Fatalf("delete: %v", err)
	}
	mid := chunkref.SnapshotID(snap.Seq)
	if got := sink.removed[mid]; len(got) != 1 || got[0] != "chunk-A" {
		t.Fatalf("RemoveRef on delete = %v, want [chunk-A]", got)
	}
}

func TestNilRefSinkIsNoop(t *testing.T) {
	backend := newFakeSnapshotableWithObjects([]storage.SnapshotObject{
		{Bucket: "bkt", Key: "k", Segments: []storage.SegmentRef{{BlobID: "chunk-A"}}},
	})
	m, err := NewManagerWithRefSink(t.TempDir(), backend, "", nil, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	snap, err := m.Create("test")
	if err != nil {
		t.Fatalf("create with nil sink must not panic: %v", err)
	}
	if err := m.Delete(snap.Seq); err != nil {
		t.Fatalf("delete with nil sink: %v", err)
	}
}
