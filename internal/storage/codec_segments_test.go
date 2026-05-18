package storage

import (
	"reflect"
	"testing"
)

func TestObjectCodecRoundTripWithSegments(t *testing.T) {
	orig := &Object{
		Key: "k", Size: 30 * 1024 * 1024, ETag: "cafef00d-3",
		Segments: []SegmentRef{
			{BlobID: "b1", Size: 10 << 20, ETag: "aa"},
			{BlobID: "b2", Size: 10 << 20, ETag: "bb"},
			{BlobID: "b3", Size: 10 << 20, ETag: "cc"},
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
