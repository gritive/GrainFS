package cluster

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

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

func TestPlanAppendCompositeETagSurvivesCoalesce(t *testing.T) {
	c1 := []byte("aaaaaaaaaaaaaaaa")
	c2 := []byte("bbbbbbbbbbbbbbbb")
	c3 := []byte("cccccccccccccccc")
	base := PutObjectMetaCmd{Bucket: "b", Key: "k", Size: 48, IsAppendable: true, Segments: nil, Coalesced: []CoalescedShardRef{{CoalescedID: "x", Size: 48, ShardKey: "k/coalesced/x"}}, AppendCallMD5s: [][]byte{c1, c2, c3}, MetaSeq: 5}
	seg := storage.SegmentRef{Size: 16, Checksum: []byte("dddddddddddddddd")}
	next, _, _, err := planAppendObjectBlobRMWWithSide(appendBlobRMWInput{Bucket: "b", Key: "k", ExpectedOffset: 48, Segment: seg, Base: base, BaseExists: true, ModifiedUnixSec: 1})
	require.NoError(t, err)
	require.Equal(t, storage.CompositeETag([][]byte{c1, c2, c3, seg.Checksum}), next.ETag, "composite ETag must include the pre-coalesce per-call digests")
	require.Equal(t, [][]byte{c1, c2, c3, seg.Checksum}, next.AppendCallMD5s)
}

func TestPlanAppendObjectBlobRMWCoalescedPrefixUsesSideRecords(t *testing.T) {
	baseState, count, err := storage.AppendETagStateAppend(nil, 0, []byte("base-segment-md5"))
	require.NoError(t, err)
	etag, err := storage.CompositeETagFromState(baseState, count)
	require.NoError(t, err)

	base := PutObjectMetaCmd{
		Bucket:           "b",
		Key:              "k",
		Size:             12,
		IsAppendable:     true,
		Segments:         nil,
		Coalesced:        []CoalescedShardRef{{CoalescedID: "c1", Size: 4, ShardKey: "k/coalesced/c1"}},
		MetaSeq:          3,
		NodeIDs:          []string{"n1"},
		ETag:             etag,
		PlacementGroupID: "g0",
		ECData:           1,
		ECParity:         0,
	}
	seg := storage.SegmentRef{Size: 2, Checksum: []byte("new-segment-md5")}
	summary := storage.AppendSummary{
		Size:            8,
		SegmentCount:    1,
		ETagPartCount:   count,
		ETagDigestState: baseState,
	}
	next, nextSummary, sideMode, err := planAppendObjectBlobRMWWithSide(appendBlobRMWInput{
		Bucket:           "b",
		Key:              "k",
		ExpectedOffset:   12,
		Segment:          seg,
		Base:             base,
		BaseExists:       true,
		ModifiedUnixSec:  1,
		UseSideRecords:   true,
		BaseSummary:      summary,
		BaseHasSummary:   true,
		PlacementGroupID: "g0",
		SizeCapBytes:     0,
	})
	require.NoError(t, err)
	require.True(t, sideMode)
	require.Len(t, next.Coalesced, 1)
	require.Empty(t, next.Segments)
	require.Equal(t, int64(14), next.Size)
	require.Empty(t, next.AppendCallMD5s)
	computedETag, err := storage.CompositeETagFromState(nextSummary.ETagDigestState, nextSummary.ETagPartCount)
	require.NoError(t, err)
	require.Equal(t, computedETag, next.ETag)
	require.Greater(t, nextSummary.SegmentCount, summary.SegmentCount)
}

func TestPlanAppendSeedsAppendCallMD5sOnFirstCall(t *testing.T) {
	seg := storage.SegmentRef{Size: 5, Checksum: []byte("0123456789abcdef")}
	next, _, _, err := planAppendObjectBlobRMWWithSide(appendBlobRMWInput{Bucket: "b", Key: "k", ExpectedOffset: 0, Segment: seg, BaseExists: false, ModifiedUnixSec: 1})
	require.NoError(t, err)
	require.Equal(t, [][]byte{seg.Checksum}, next.AppendCallMD5s)
	require.Equal(t, storage.CompositeETag([][]byte{seg.Checksum}), next.ETag)
}

func TestPlanCoalescePreservesAppendCallMD5s(t *testing.T) {
	base := PutObjectMetaCmd{Bucket: "b", Key: "k", IsAppendable: true, Segments: []SegmentMetaEntry{{BlobID: "s1"}}, AppendCallMD5s: [][]byte{[]byte("aaaaaaaaaaaaaaaa")}}
	next, res, err := planCoalesceBlobRMW(base, CoalesceSegmentsPlan{Bucket: "b", Key: "k", CoalescedID: "x", ShardKey: "k/coalesced/x", ConsumedSegmentIDs: []string{"s1"}})
	require.NoError(t, err)
	require.False(t, res.Noop)
	require.Equal(t, base.AppendCallMD5s, next.AppendCallMD5s, "coalesce must carry the per-call digest history forward unchanged")
}

// TestPlanAppendFirstAppendOntoChunkedPutPreservesETag: first append onto a
// CHUNKED PUT base (Segments populated, AppendCallMD5s empty) must keep the
// composite ETag byte-identical to the pre-fix Segments-derived value.
func TestPlanAppendFirstAppendOntoChunkedPutPreservesETag(t *testing.T) {
	s1 := []byte("1111111111111111")
	s2 := []byte("2222222222222222")
	base := PutObjectMetaCmd{Bucket: "b", Key: "k", Size: 32, IsAppendable: false, Segments: []SegmentMetaEntry{{BlobID: "s1", Size: 16, Checksum: s1}, {BlobID: "s2", Size: 16, Checksum: s2}}, AppendCallMD5s: nil, MetaSeq: 2}
	seg := storage.SegmentRef{Size: 16, Checksum: []byte("3333333333333333")}
	next, _, _, err := planAppendObjectBlobRMWWithSide(appendBlobRMWInput{Bucket: "b", Key: "k", ExpectedOffset: 32, Segment: seg, Base: base, BaseExists: true, ModifiedUnixSec: 1})
	require.NoError(t, err)
	require.Equal(t, storage.CompositeETag([][]byte{s1, s2, seg.Checksum}), next.ETag, "first append onto a chunked PUT must seed history from base.Segments")
	require.Equal(t, [][]byte{s1, s2, seg.Checksum}, next.AppendCallMD5s)
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
