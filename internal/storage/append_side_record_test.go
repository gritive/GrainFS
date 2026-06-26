package storage

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestHeadObjectLoadsAppendSideSegments(t *testing.T) {
	b, obj := seedAppendSideRecordObject(t, []string{"hello ", "world"})

	got, err := b.HeadObject(context.Background(), "test", "k")
	require.NoError(t, err, "HeadObject")
	require.Equal(t, obj.Size, got.Size)
	require.Len(t, got.Segments, 2)
	require.Equal(t, obj.Segments[0].BlobID, got.Segments[0].BlobID)
	require.Equal(t, obj.Segments[1].BlobID, got.Segments[1].BlobID)
}

func TestGetObjectReadsAppendSideSegments(t *testing.T) {
	b, _ := seedAppendSideRecordObject(t, []string{"hello ", "world"})

	rc, obj, err := b.GetObject(context.Background(), "test", "k")
	require.NoError(t, err, "GetObject")
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err, "ReadAll")
	require.Equal(t, "hello world", string(got))
	require.Len(t, obj.Segments, 2)
}

func TestHeadObjectAppendSideRecordMissingSummaryFailsClosed(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()
	obj := &Object{Key: "k", Size: 5, ETag: "etag-1", IsAppendable: true}
	require.NoError(t, b.PutObjectRecord(ctx, "test", "k", obj), "PutObjectRecord")

	_, err := b.HeadObject(ctx, "test", "k")
	require.Error(t, err, "HeadObject succeeded for side-record object with missing append summary")
}

func TestAppendSummaryRoundTripsLegacyHeaderOnlyEncoding(t *testing.T) {
	encoded := encodeAppendSummary(appendSummary{Size: 123, SegmentCount: 4})
	require.Len(t, encoded, 16)

	got, err := decodeAppendSummary(encoded)
	require.NoError(t, err)
	require.Equal(t, appendSummary{Size: 123, SegmentCount: 4}, got)
}

func TestAppendSummaryRoundTripsETagState(t *testing.T) {
	want := appendSummary{
		Size:            123,
		SegmentCount:    4,
		ETagPartCount:   4,
		ETagDigestState: []byte("md5-state"),
	}
	encoded := encodeAppendSummary(want)
	require.Len(t, encoded, 28+len(want.ETagDigestState))

	got, err := decodeAppendSummary(encoded)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func seedAppendSideRecordObject(t *testing.T, parts []string) (*LocalBackend, *Object) {
	t.Helper()
	b := newTestLocalBackend(t)
	ctx := context.Background()
	segs := make([]SegmentRef, 0, len(parts))
	var size int64
	for _, p := range parts {
		seg, err := b.WriteSegmentBlob("test", "k", strings.NewReader(p))
		require.NoError(t, err, "WriteSegmentBlob")
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
	require.NoError(t, b.PutObjectRecord(ctx, "test", "k", obj), "PutObjectRecord")
	err := b.db.Update(func(txn *badger.Txn) error {
		return b.writeAppendSideRecordsInTxn(txn, "test", "k", "", appendSummary{Size: size, SegmentCount: len(segs)}, segs)
	})
	require.NoError(t, err, "writeAppendSideRecords")
	full := *obj
	full.Segments = append([]SegmentRef(nil), segs...)
	return b, &full
}
