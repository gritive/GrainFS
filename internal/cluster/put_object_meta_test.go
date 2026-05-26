package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestBuildPutObjectMeta(t *testing.T) {
	cmd := PutObjectMetaCmd{
		Key:              "k",
		Size:             12,
		ContentType:      "text/plain",
		ETag:             "etag-1",
		ModTime:          1234,
		ECData:           2,
		ECParity:         1,
		NodeIDs:          []string{"n1", "n2", "n3"},
		PlacementGroupID: "pg-1",
		UserMetadata:     map[string]string{"a": "b"},
		SSEAlgorithm:     "AES256",
		Parts:            []storage.MultipartPartEntry{{PartNumber: 1, Size: 12}},
		Segments: []SegmentMetaEntry{{
			BlobID:   "seg-1",
			Size:     12,
			Checksum: []byte{1, 2, 3},
			NodeIDs:  []string{"n1"},
		}},
		Tags: []storage.Tag{{Key: "tag", Value: "value"}},
	}

	meta := buildPutObjectMeta(cmd)
	if meta.Key != "k" || meta.Size != 12 || meta.ContentType != "text/plain" || meta.ETag != "etag-1" {
		t.Fatalf("basic fields = %+v", meta)
	}
	if meta.LastModified != 1234 || meta.ECData != 2 || meta.ECParity != 1 || meta.PlacementGroupID != "pg-1" {
		t.Fatalf("placement fields = %+v", meta)
	}
	if len(meta.NodeIDs) != 3 || meta.NodeIDs[0] != "n1" || meta.UserMetadata["a"] != "b" || meta.SSEAlgorithm != "AES256" {
		t.Fatalf("metadata fields = %+v", meta)
	}
	if len(meta.Parts) != 1 || meta.Parts[0].PartNumber != 1 {
		t.Fatalf("parts = %+v", meta.Parts)
	}
	if len(meta.Segments) != 1 || meta.Segments[0].BlobID != "seg-1" || meta.Segments[0].Checksum[0] != 1 {
		t.Fatalf("segments = %+v", meta.Segments)
	}
	if len(meta.Tags) != 1 || meta.Tags[0].Key != "tag" {
		t.Fatalf("tags = %+v", meta.Tags)
	}
}

func TestBuildPutObjectMetaDeleteMarkerUsesSentinelETag(t *testing.T) {
	meta := buildPutObjectMeta(PutObjectMetaCmd{
		Key:            "k",
		ETag:           "ignored",
		IsDeleteMarker: true,
	})
	if meta.ETag != deleteMarkerETag {
		t.Fatalf("ETag=%q want %q", meta.ETag, deleteMarkerETag)
	}
}
