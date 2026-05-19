package storage

import (
	"bytes"
	"reflect"
	"testing"
)

func TestObjectCodecRoundTripWithSegments(t *testing.T) {
	orig := &Object{
		Key: "k", Size: 30 * 1024 * 1024, ETag: "cafef00d-3",
		Segments: []SegmentRef{
			{BlobID: "b1", Size: 10 << 20, Checksum: bytes.Repeat([]byte{0xaa}, ChecksumLen), PlacementGroupID: "pg1", ShardSize: 1 << 20},
			{BlobID: "b2", Size: 10 << 20, Checksum: bytes.Repeat([]byte{0xbb}, ChecksumLen), PlacementGroupID: "pg1", ShardSize: 1 << 20},
			{BlobID: "b3", Size: 10 << 20, Checksum: bytes.Repeat([]byte{0xcc}, ChecksumLen), PlacementGroupID: "pg2", ShardSize: 1 << 20},
		},
		IsAppendable: true,
	}
	data, err := marshalObject(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := unmarshalObject(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(got.Segments, orig.Segments) {
		t.Fatalf("segments mismatch: got=%v want=%v", got.Segments, orig.Segments)
	}
	if got.IsAppendable != orig.IsAppendable {
		t.Fatalf("is_appendable mismatch: got=%v want=true", got.IsAppendable)
	}
}

func TestObjectCodecLegacyHasNoSegments(t *testing.T) {
	orig := &Object{Key: "legacy", Size: 100, ETag: "x"}
	data, err := marshalObject(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := unmarshalObject(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Segments != nil {
		t.Fatalf("expected nil segments, got %v", got.Segments)
	}
	if got.IsAppendable {
		t.Fatal("expected IsAppendable false for legacy")
	}
}
