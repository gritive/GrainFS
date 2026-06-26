package storage

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestHeadObjectLoadsAppendSideSegments(t *testing.T) {
	b, obj := seedAppendSideRecordObject(t, []string{"hello ", "world"})

	got, err := b.HeadObject(context.Background(), "test", "k")
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if got.Size != obj.Size {
		t.Fatalf("size=%d, want %d", got.Size, obj.Size)
	}
	if len(got.Segments) != 2 {
		t.Fatalf("segments=%d, want 2", len(got.Segments))
	}
	if got.Segments[0].BlobID != obj.Segments[0].BlobID || got.Segments[1].BlobID != obj.Segments[1].BlobID {
		t.Fatalf("segments=%v, want %v", got.Segments, obj.Segments)
	}
}

func TestGetObjectReadsAppendSideSegments(t *testing.T) {
	b, _ := seedAppendSideRecordObject(t, []string{"hello ", "world"})

	rc, obj, err := b.GetObject(context.Background(), "test", "k")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "hello world" {
		t.Fatalf("body=%q, want hello world", string(got))
	}
	if len(obj.Segments) != 2 {
		t.Fatalf("segments=%d, want 2", len(obj.Segments))
	}
}

func TestHeadObjectAppendSideRecordMissingSummaryFailsClosed(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()
	obj := &Object{Key: "k", Size: 5, ETag: "etag-1", IsAppendable: true}
	if err := b.PutObjectRecord(ctx, "test", "k", obj); err != nil {
		t.Fatalf("PutObjectRecord: %v", err)
	}

	if _, err := b.HeadObject(ctx, "test", "k"); err == nil {
		t.Fatal("HeadObject succeeded for side-record object with missing append summary")
	}
}

func seedAppendSideRecordObject(t *testing.T, parts []string) (*LocalBackend, *Object) {
	t.Helper()
	b := newTestLocalBackend(t)
	ctx := context.Background()
	segs := make([]SegmentRef, 0, len(parts))
	var size int64
	for _, p := range parts {
		seg, err := b.WriteSegmentBlob("test", "k", strings.NewReader(p))
		if err != nil {
			t.Fatalf("WriteSegmentBlob: %v", err)
		}
		segs = append(segs, seg)
		size += seg.Size
	}
	obj := &Object{
		Key:          "k",
		Size:         size,
		ContentType:  "application/octet-stream",
		ETag:         "side-etag-2",
		IsAppendable: true,
	}
	if err := b.PutObjectRecord(ctx, "test", "k", obj); err != nil {
		t.Fatalf("PutObjectRecord: %v", err)
	}
	if err := b.writeAppendSideRecords(ctx, "test", "k", "", appendSummary{Size: size, SegmentCount: len(segs)}, segs); err != nil {
		t.Fatalf("writeAppendSideRecords: %v", err)
	}
	full := *obj
	full.Segments = append([]SegmentRef(nil), segs...)
	return b, &full
}
