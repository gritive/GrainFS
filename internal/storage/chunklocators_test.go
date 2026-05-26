package storage

import (
	"reflect"
	"testing"
)

func TestObjectChunkLocatorsCoversSegmentsAndCoalesced(t *testing.T) {
	o := &Object{
		Segments: []SegmentRef{
			{BlobID: "0192f3c0-aaaa-7bbb-8ccc-000000000001"},
			{BlobID: "cas://b3-deadbeef"},
		},
		Coalesced: []CoalescedRef{
			{CoalescedID: "0192f3c0-dddd-7eee-8fff-000000000002"},
		},
	}
	got := o.ChunkLocators()
	want := []string{
		"0192f3c0-aaaa-7bbb-8ccc-000000000001",
		"cas://b3-deadbeef",
		"0192f3c0-dddd-7eee-8fff-000000000002",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ChunkLocators() = %v, want %v", got, want)
	}
}

func TestSnapshotObjectChunkLocatorsCoversSegmentsAndCoalesced(t *testing.T) {
	so := &SnapshotObject{
		Segments:  []SegmentRef{{BlobID: "legacy://bkt/key/blob-1"}},
		Coalesced: []CoalescedRef{{CoalescedID: "coalesced-1"}},
	}
	got := so.ChunkLocators()
	want := []string{"blob-1", "coalesced-1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ChunkLocators() = %v, want %v", got, want)
	}
}

func TestChunkLocatorsEmpty(t *testing.T) {
	if got := (&Object{}).ChunkLocators(); got != nil {
		t.Fatalf("empty object ChunkLocators = %v, want nil", got)
	}
}
