package cluster

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestPlanAppendObjectAdmission(t *testing.T) {
	existing := &storage.Object{
		Size:     10,
		Segments: []storage.SegmentRef{{BlobID: "seg-1"}},
	}
	full := &storage.Object{
		Size:     10,
		Segments: make([]storage.SegmentRef, storage.MaxAppendSegments),
	}

	tests := []struct {
		name string
		in   appendObjectAdmissionInput
		want error
	}{
		{
			name: "missing object accepts zero offset",
			in: appendObjectAdmissionInput{
				ExpectedOffset: 0,
				ChunkSize:      4,
				SizeCapBytes:   8,
			},
		},
		{
			name: "missing object rejects nonzero offset",
			in: appendObjectAdmissionInput{
				ExpectedOffset: 4,
				ChunkSize:      4,
				SizeCapBytes:   8,
			},
			want: storage.ErrAppendOffsetMismatch,
		},
		{
			name: "existing rejects offset mismatch",
			in: appendObjectAdmissionInput{
				Existing:       existing,
				ExpectedOffset: 9,
				ChunkSize:      1,
				SizeCapBytes:   0,
			},
			want: storage.ErrAppendOffsetMismatch,
		},
		{
			name: "existing rejects segment cap",
			in: appendObjectAdmissionInput{
				Existing:       full,
				ExpectedOffset: 10,
				ChunkSize:      1,
				SizeCapBytes:   0,
			},
			want: storage.ErrAppendCapExceeded,
		},
		{
			name: "existing rejects conservative size cap",
			in: appendObjectAdmissionInput{
				Existing:       existing,
				ExpectedOffset: 10,
				ChunkSize:      3,
				SizeCapBytes:   12,
			},
			want: storage.ErrAppendObjectTooLarge,
		},
		{
			name: "existing allows exact size cap",
			in: appendObjectAdmissionInput{
				Existing:       existing,
				ExpectedOffset: 10,
				ChunkSize:      2,
				SizeCapBytes:   12,
			},
		},
		{
			name: "unknown chunk size skips size cap",
			in: appendObjectAdmissionInput{
				Existing:       existing,
				ExpectedOffset: 10,
				ChunkSize:      -1,
				SizeCapBytes:   12,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := planAppendObjectAdmission(tt.in)
			if !errors.Is(err, tt.want) {
				t.Fatalf("planAppendObjectAdmission() error=%v want %v", err, tt.want)
			}
		})
	}
}

func TestAppendChunkSizeRestoresSeekPosition(t *testing.T) {
	r := bytes.NewReader([]byte("abcdef"))
	if _, err := r.Seek(2, io.SeekStart); err != nil {
		t.Fatalf("Seek: %v", err)
	}

	if got := appendChunkSize(r); got != 4 {
		t.Fatalf("appendChunkSize()=%d want 4", got)
	}
	pos, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatalf("Seek current: %v", err)
	}
	if pos != 2 {
		t.Fatalf("reader position=%d want 2", pos)
	}
}

func TestBuildAppendObjectCommand(t *testing.T) {
	seg := storage.SegmentRef{
		BlobID:   "blob-1",
		Size:     42,
		Checksum: []byte{0xde, 0xad, 0xbe, 0xef},
	}

	cmd := buildAppendObjectCommand(appendObjectCommandInput{
		Bucket:           "b",
		Key:              "k",
		ExpectedOffset:   10,
		Segment:          seg,
		PlacementGroupID: "pg-1",
		VersionID:        "version-1",
		ModifiedUnixSec:  1234,
	})

	if cmd.Bucket != "b" || cmd.Key != "k" || cmd.ExpectedOffset != 10 {
		t.Fatalf("command target fields = %+v", cmd)
	}
	if cmd.BlobID != seg.BlobID || cmd.SegmentSize != seg.Size || cmd.SegmentETag != "deadbeef" {
		t.Fatalf("command segment fields = %+v", cmd)
	}
	if cmd.PlacementGroupID != "pg-1" || cmd.VersionID != "version-1" || cmd.ModifiedUnixSec != 1234 {
		t.Fatalf("command metadata fields = %+v", cmd)
	}
}

func TestApplyAppendObjectTransitionCreatesFirstAppendableObject(t *testing.T) {
	updated, result, err := applyAppendObjectTransition(appendObjectTransitionInput{
		Cmd: AppendObjectCmd{
			Key:              "k",
			ExpectedOffset:   0,
			BlobID:           "blob-1",
			SegmentSize:      4,
			SegmentETag:      "deadbeef",
			PlacementGroupID: "pg-1",
		},
		ModifiedUnixSec: 1234,
	})
	if err != nil {
		t.Fatalf("applyAppendObjectTransition: %v", err)
	}
	if result.Noop || result.SizeCapRejected {
		t.Fatalf("unexpected result flags: %+v", result)
	}
	if updated.Key != "k" || updated.Size != 4 || updated.ContentType != "application/octet-stream" {
		t.Fatalf("updated target fields = %+v", updated)
	}
	if updated.ETag != storage.CompositeETag([][]byte{{0xde, 0xad, 0xbe, 0xef}}) {
		t.Fatalf("updated ETag=%q", updated.ETag)
	}
	if updated.LastModified != 1234 || updated.PlacementGroupID != "pg-1" || !updated.IsAppendable {
		t.Fatalf("updated metadata fields = %+v", updated)
	}
	if len(updated.Segments) != 1 || updated.Segments[0].BlobID != "blob-1" || updated.Segments[0].Size != 4 {
		t.Fatalf("updated segments = %+v", updated.Segments)
	}
}

func TestApplyAppendObjectTransitionConvertsPlainObject(t *testing.T) {
	existing := &objectMeta{
		Key:          "k",
		Size:         5,
		ContentType:  "text/plain",
		ETag:         "base-etag",
		ECData:       2,
		ECParity:     1,
		NodeIDs:      []string{"n1", "n2", "n3"},
		UserMetadata: map[string]string{"a": "b"},
	}

	updated, result, err := applyAppendObjectTransition(appendObjectTransitionInput{
		Existing:          existing,
		ExistingVersionID: "v1",
		Cmd: AppendObjectCmd{
			Key:            "k",
			ExpectedOffset: 5,
			BlobID:         "blob-2",
			SegmentSize:    3,
			SegmentETag:    "01020304",
		},
		ModifiedUnixSec: 5678,
	})
	if err != nil {
		t.Fatalf("applyAppendObjectTransition: %v", err)
	}
	if result.Noop || result.SizeCapRejected {
		t.Fatalf("unexpected result flags: %+v", result)
	}
	if updated.Size != 8 || !updated.IsAppendable || updated.LastModified != 5678 {
		t.Fatalf("updated fields = %+v", updated)
	}
	if len(updated.Coalesced) != 1 || updated.Coalesced[0].CoalescedID != "base-v1" ||
		updated.Coalesced[0].Size != 5 || updated.Coalesced[0].ShardKey != ecObjectShardKey("k", "v1") {
		t.Fatalf("updated coalesced = %+v", updated.Coalesced)
	}
	if len(updated.Segments) != 1 || updated.Segments[0].BlobID != "blob-2" {
		t.Fatalf("updated segments = %+v", updated.Segments)
	}
}

func TestApplyAppendObjectTransitionReportsNoopAndSizeCap(t *testing.T) {
	existing := &objectMeta{
		Key:          "k",
		Size:         5,
		Segments:     []storage.SegmentRef{{BlobID: "blob-1", Size: 5}},
		IsAppendable: true,
	}

	_, result, err := applyAppendObjectTransition(appendObjectTransitionInput{
		Existing: existing,
		Cmd: AppendObjectCmd{
			ExpectedOffset: 5,
			BlobID:         "blob-1",
			SegmentSize:    5,
			SegmentETag:    "01020304",
		},
	})
	if err != nil {
		t.Fatalf("idempotent transition error: %v", err)
	}
	if !result.Noop {
		t.Fatalf("Noop=false for duplicate blob: %+v", result)
	}

	_, result, err = applyAppendObjectTransition(appendObjectTransitionInput{
		Existing: existing,
		Cmd: AppendObjectCmd{
			ExpectedOffset: 5,
			BlobID:         "blob-2",
			SegmentSize:    6,
			SegmentETag:    "01020304",
		},
		CoalesceCfg: CoalesceConfig{SizeCapBytes: 10},
	})
	if !errors.Is(err, storage.ErrAppendObjectTooLarge) {
		t.Fatalf("size cap error=%v", err)
	}
	if !result.SizeCapRejected {
		t.Fatalf("SizeCapRejected=false: %+v", result)
	}
}
