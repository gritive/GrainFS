package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestChunkedSegmentStore_RoutesAndReads(t *testing.T) {
	b, bucket, key, body := putChunkedTestObject(t)
	obj, err := b.HeadObject(context.Background(), bucket, key)
	require.NoError(t, err)
	require.NotEmpty(t, obj.Segments)

	t.Run("OpenSegment_LocalDataShard", func(t *testing.T) {
		store := &clusterSegmentStore{b: b, bucket: bucket, key: key, obj: obj}
		rc, err := store.OpenSegment(context.Background(), obj.Segments[0])
		require.NoError(t, err)
		defer rc.Close()

		got, err := io.ReadAll(rc)
		require.NoError(t, err)
		require.Equal(t, body[:obj.Segments[0].Size], got)
	})

	t.Run("GetObject_ChunkedRoutesToSegmentStore", func(t *testing.T) {
		rc, gotObj, err := b.GetObject(context.Background(), bucket, key)
		require.NoError(t, err)
		require.NotNil(t, gotObj)
		defer rc.Close()

		got, err := io.ReadAll(rc)
		require.NoError(t, err)
		require.Equal(t, body, got)
	})

	t.Run("GetObjectVersion_ChunkedRoutesToSegmentStore", func(t *testing.T) {
		require.NotEmpty(t, obj.VersionID)

		versionHead, err := b.HeadObjectVersion(context.Background(), bucket, key, obj.VersionID)
		require.NoError(t, err)
		require.NotEmpty(t, versionHead.Segments)

		rc, gotObj, err := b.GetObjectVersion(context.Background(), bucket, key, obj.VersionID)
		require.NoError(t, err)
		require.NotNil(t, gotObj)
		defer rc.Close()

		got, err := io.ReadAll(rc)
		require.NoError(t, err)
		require.Equal(t, body, got)
	})

	t.Run("ReadAt_CrossSegmentBoundary", func(t *testing.T) {
		offset := obj.Segments[0].Size - 3
		buf := make([]byte, 8)

		n, err := b.ReadAt(context.Background(), bucket, key, offset, buf)
		require.NoError(t, err)
		require.Equal(t, len(buf), n)
		require.Equal(t, body[offset:offset+int64(len(buf))], buf)
	})
}

func TestChunkedSegmentStore_OpenSegmentLargeSegmentStreamsExactBytes(t *testing.T) {
	b := setupECBackend(t)
	b.chunkedPutChunkSize = 10 << 20
	b.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-a": {ID: "group-a", PeerIDs: []string{"self", "self", "self"}},
	}})

	const (
		bucket = "chunked-bucket"
		key    = "large-streamed-object"
	)
	body := makeChunkedTestBody(10 << 20)
	sp := makeSpool(t, body)

	require.NoError(t, b.CreateBucket(context.Background(), bucket))
	_, err := b.putObjectChunked(context.Background(),
		bucket, key, "v1", sp, "application/octet-stream",
		nil, "", 0, 0, false, "", nil, nil, nil)
	require.NoError(t, err)
	obj, err := b.HeadObject(context.Background(), bucket, key)
	require.NoError(t, err)
	require.Len(t, obj.Segments, 1)
	require.Greater(t, obj.Segments[0].Size, int64(maxECPooledReadObjectSize))

	store := &clusterSegmentStore{b: b, bucket: bucket, key: key, obj: obj}
	rc, err := store.OpenSegment(context.Background(), obj.Segments[0])
	require.NoError(t, err)
	defer rc.Close()

	_, buffered := rc.(interface{ SegmentBytes() []byte })
	require.False(t, buffered, "large segment OpenSegment must stream instead of returning a full-buffer provider")

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, body, got)
}

func TestChunkedSegmentStore_OpenSegmentRejectsMetadataSizeMismatch(t *testing.T) {
	b, bucket, key, _ := putChunkedTestObject(t)
	obj, err := b.HeadObject(context.Background(), bucket, key)
	require.NoError(t, err)
	require.NotEmpty(t, obj.Segments)
	require.Greater(t, obj.Segments[0].Size, int64(1))

	for _, tc := range []struct {
		name    string
		delta   int64
		wantErr string
	}{
		{name: "metadata_too_small", delta: -1, wantErr: "exceeds metadata size"},
		{name: "metadata_too_large", delta: 1, wantErr: "reconstructed size short"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			badObj := *obj
			badObj.Segments = append([]storage.SegmentRef(nil), obj.Segments...)
			badObj.Segments[0].Size += tc.delta

			store := &clusterSegmentStore{b: b, bucket: bucket, key: key, obj: &badObj}
			rc, err := store.OpenSegment(context.Background(), badObj.Segments[0])
			require.NoError(t, err)
			_, err = io.ReadAll(rc)
			require.ErrorContains(t, err, tc.wantErr)
			require.NoError(t, rc.Close())
		})
	}
}

func TestCompleteMultipartUpload_ChunkedObjectPreservesParts(t *testing.T) {
	b := setupECBackend(t)
	b.chunkedPutChunkSize = testChunkedMultipartChunkSize
	b.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-a": {ID: "group-a", PeerIDs: []string{"self", "self", "self"}},
	}})

	const (
		bucket = "chunked-multipart-bucket"
		key    = "large-multipart-object"
	)
	part1 := makeChunkedTestBody(testChunkedMultipartChunkSize)
	part2 := makeChunkedTestBody(4096)
	want := append(append([]byte(nil), part1...), part2...)

	require.NoError(t, b.CreateBucket(context.Background(), bucket))
	upload, err := b.CreateMultipartUpload(context.Background(), bucket, key, "application/octet-stream")
	require.NoError(t, err)

	p1, err := b.UploadPart(context.Background(), bucket, key, upload.UploadID, 1, bytes.NewReader(part1), "")
	require.NoError(t, err)
	p2, err := b.UploadPart(context.Background(), bucket, key, upload.UploadID, 2, bytes.NewReader(part2), "")
	require.NoError(t, err)

	obj, err := b.CompleteMultipartUpload(context.Background(), bucket, key, upload.UploadID, []storage.Part{*p1, *p2})
	require.NoError(t, err)
	require.Equal(t, int64(len(want)), obj.Size)
	require.Len(t, obj.Segments, 2)
	require.Len(t, obj.Parts, 2)

	head, err := b.HeadObject(context.Background(), bucket, key)
	require.NoError(t, err)
	require.Len(t, head.Segments, 2)
	require.Len(t, head.Parts, 2)

	rc, gotObj, err := b.GetObject(context.Background(), bucket, key)
	require.NoError(t, err)
	require.Len(t, gotObj.Parts, 2)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// TestCompleteMultipartUpload_SmallNowChunks proves the single multipart path:
// a SMALL multipart completion (below the old chunk threshold) now also takes
// the chunked path (obj.Segments non-empty), preserves Parts + whole-object MD5
// ETag, round-trips, and removes the staged part dir.
func TestCompleteMultipartUpload_SmallNowChunks(t *testing.T) {
	mkBackend := func(t *testing.T) *DistributedBackend {
		b := setupECBackend(t)
		b.chunkedPutChunkSize = testChunkedMultipartChunkSize
		b.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
			"group-a": {ID: "group-a", PeerIDs: []string{"self", "self", "self"}},
		}})
		return b
	}

	// NOTE: a genuinely-small MULTI-part completion is impossible — S3 requires
	// every part except the last to be >= 5 MiB. Small single-part and zero-byte
	// completions are the meaningful small cases; large multi-part chunking is
	// covered by TestCompleteMultipartUpload_ChunkedObjectPreservesParts.
	for _, tc := range []struct {
		name  string
		parts [][]byte
	}{
		{"single_small_part", [][]byte{[]byte("small-single-part-body")}},
		{"single_zero_byte_part", [][]byte{{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := mkBackend(t)
			ctx := context.Background()
			bucket, key := "mp-small-"+tc.name, "obj"
			require.NoError(t, b.CreateBucket(ctx, bucket))
			up, err := b.CreateMultipartUpload(ctx, bucket, key, "application/octet-stream")
			require.NoError(t, err)

			var want []byte
			var completed []storage.Part
			for i, p := range tc.parts {
				part, perr := b.UploadPart(ctx, bucket, key, up.UploadID, i+1, bytes.NewReader(p), "")
				require.NoError(t, perr)
				completed = append(completed, *part)
				want = append(want, p...)
			}

			obj, err := b.CompleteMultipartUpload(ctx, bucket, key, up.UploadID, completed)
			require.NoError(t, err)
			require.NotEmpty(t, obj.Segments, "small multipart now takes the chunked path")
			require.Len(t, obj.Parts, len(tc.parts), "Parts metadata preserved")
			sum := md5.Sum(want)
			require.Equal(t, hex.EncodeToString(sum[:]), obj.ETag, "multipart ETag = whole-object md5")

			_, statErr := os.Stat(b.partDir(up.UploadID))
			require.True(t, os.IsNotExist(statErr), "staged part dir removed after complete")

			rc, _, gerr := b.GetObject(ctx, bucket, key)
			require.NoError(t, gerr)
			defer rc.Close()
			got, rerr := io.ReadAll(rc)
			require.NoError(t, rerr)
			require.True(t, bytes.Equal(want, got), "multipart object round-trips")
		})
	}
}

func TestGetObject_AppendableNotRoutedToChunked(t *testing.T) {
	b := setupECBackend(t)
	ctx := context.Background()
	const (
		bucket = "append-bucket"
		key    = "append-object"
	)
	require.NoError(t, b.CreateBucket(ctx, bucket))

	_, err := b.AppendObject(ctx, bucket, key, 0, bytes.NewReader([]byte("hello ")))
	require.NoError(t, err)
	obj, err := b.AppendObject(ctx, bucket, key, 6, bytes.NewReader([]byte("world")))
	require.NoError(t, err)
	require.True(t, obj.IsAppendable)
	require.Empty(t, obj.Segments)

	rc, gotObj, err := b.GetObject(ctx, bucket, key)
	require.NoError(t, err)
	require.True(t, gotObj.IsAppendable)
	require.Len(t, gotObj.Segments, 2)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, []byte("hello world"), got)
}

func TestOpenSegment_CompressedRoundTrip(t *testing.T) {
	b := setupECBackend(t)
	b.chunkedPutChunkSize = testChunkedPutChunkSize
	b.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-a": {ID: "group-a", PeerIDs: []string{"self", "self", "self"}},
	}})

	const (
		bucket = "compressed-roundtrip-bucket"
		key    = "compressed-object"
	)
	// Highly compressible: zstd will produce StoredSize << Size.
	plaintext := bytes.Repeat([]byte("zstd-roundtrip "), 16384)
	sp := makeSpool(t, plaintext)

	require.NoError(t, b.CreateBucket(context.Background(), bucket))
	_, err := b.putObjectChunked(context.Background(),
		bucket, key, "v1", sp, "application/octet-stream",
		nil, "", 0, 0, false, "", nil, nil, nil)
	require.NoError(t, err)

	obj, err := b.HeadObject(context.Background(), bucket, key)
	require.NoError(t, err)
	require.NotEmpty(t, obj.Segments)
	require.Greater(t, obj.Segments[0].StoredSize, int64(0), "precondition: expected compressed segment")

	store := &clusterSegmentStore{b: b, bucket: bucket, key: key, obj: obj}
	rc, err := store.OpenSegment(context.Background(), obj.Segments[0])
	require.NoError(t, err)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, plaintext[:obj.Segments[0].Size], got)
}

func TestChunkedSegmentWindow_SelectsOnlyOverlappingSegments(t *testing.T) {
	refs := []storage.SegmentRef{
		{BlobID: "seg-0", Size: 10},
		{BlobID: "seg-1", Size: 10},
		{BlobID: "seg-2", Size: 10},
		{BlobID: "seg-3", Size: 10},
	}
	got, startOff, err := chunkedSegmentWindow(refs, 18, 5)
	require.NoError(t, err)
	require.Equal(t, int64(8), startOff)
	require.Equal(t, []storage.SegmentRef{refs[1], refs[2]}, got)
}

func TestReadAtChunkedSegmentsReadsOnlyOverlappingRanges(t *testing.T) {
	refs := []storage.SegmentRef{
		{BlobID: "seg-0", Size: 10},
		{BlobID: "seg-1", Size: 10},
		{BlobID: "seg-2", Size: 10},
	}
	store := &recordingSegmentRangeStore{
		data: map[string][]byte{
			"seg-0": []byte("0123456789"),
			"seg-1": []byte("abcdefghij"),
			"seg-2": []byte("ABCDEFGHIJ"),
		},
	}
	buf := make([]byte, 7)

	n, err := readAtChunkedSegments(context.Background(), store, refs, 8, buf)
	require.NoError(t, err)
	require.Equal(t, 7, n)
	require.Equal(t, []byte("89abcde"), buf)
	require.Equal(t, []segmentRangeRead{
		{blobID: "seg-0", offset: 8, length: 2},
		{blobID: "seg-1", offset: 0, length: 5},
	}, store.reads)
}

type segmentRangeRead struct {
	blobID string
	offset int64
	length int
}

type recordingSegmentRangeStore struct {
	data  map[string][]byte
	reads []segmentRangeRead
}

func (s *recordingSegmentRangeStore) ReadAtSegment(ctx context.Context, ref storage.SegmentRef, offset int64, buf []byte) (int, error) {
	_ = ctx
	s.reads = append(s.reads, segmentRangeRead{blobID: ref.BlobID, offset: offset, length: len(buf)})
	data := s.data[ref.BlobID]
	if offset >= int64(len(data)) {
		return 0, io.EOF
	}
	n := copy(buf, data[offset:])
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

func TestClusterSegmentStore_PlacementRecordUsesSegmentMetadata(t *testing.T) {
	store := &clusterSegmentStore{}
	// Recorded NodeIDs are arbitrary and NOT HRW-derivable from any key — reads
	// must replay them verbatim and never recompute placement. This is the read
	// regression guard for the write-side modulo→HRW swap: changing the write
	// algorithm cannot affect already-written objects because the read path is
	// record-driven (see internal/cluster/generation_placement.go).
	ref := storage.SegmentRef{
		BlobID:   "seg-1",
		ECData:   2,
		ECParity: 1,
		NodeIDs:  []string{"n1", "n2", "n3"},
	}
	rec, err := store.placementRecord(ref)
	require.NoError(t, err)
	require.Equal(t, PlacementRecord{Nodes: []string{"n1", "n2", "n3"}, K: 2, M: 1}, rec)
}

func TestClusterSegmentStore_PlacementRecordRequiresStoredPlacement(t *testing.T) {
	store := &clusterSegmentStore{}
	_, err := store.placementRecord(storage.SegmentRef{BlobID: "seg-1", PlacementGroupID: "pg-old"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing EC placement metadata")
}

func putChunkedTestObject(t *testing.T) (*DistributedBackend, string, string, []byte) {
	t.Helper()
	b := setupECBackend(t)
	b.chunkedPutChunkSize = testChunkedPutChunkSize
	b.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-a": {ID: "group-a", PeerIDs: []string{"self", "self", "self"}},
	}})

	const (
		bucket = "chunked-bucket"
		key    = "large-object"
	)
	body := makeChunkedTestBody(testChunkedPutChunkSize + 4096)
	sp := makeSpool(t, body)

	require.NoError(t, b.CreateBucket(context.Background(), bucket))
	_, err := b.putObjectChunked(context.Background(),
		bucket, key, "v1", sp, "application/octet-stream",
		nil, "", 0, 0, false, "", nil, nil, nil)
	require.NoError(t, err)

	return b, bucket, key, body
}

func TestReadAtSegment_CompressedOffset(t *testing.T) {
	b := setupECBackend(t)
	b.chunkedPutChunkSize = testChunkedPutChunkSize
	b.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-a": {ID: "group-a", PeerIDs: []string{"self", "self", "self"}},
	}})

	const (
		bucket = "compressed-readat-bucket"
		key    = "compressed-readat-object"
	)
	// Highly compressible: zstd will produce StoredSize << Size.
	plaintext := bytes.Repeat([]byte("0123456789"), 20000) // 200 KiB
	sp := makeSpool(t, plaintext)

	require.NoError(t, b.CreateBucket(context.Background(), bucket))
	_, err := b.putObjectChunked(context.Background(),
		bucket, key, "v1", sp, "application/octet-stream",
		nil, "", 0, 0, false, "", nil, nil, nil)
	require.NoError(t, err)

	obj, err := b.HeadObject(context.Background(), bucket, key)
	require.NoError(t, err)
	require.NotEmpty(t, obj.Segments)
	require.Greater(t, obj.Segments[0].StoredSize, int64(0), "precondition: expected compressed segment")

	store := &clusterSegmentStore{b: b, bucket: bucket, key: key, obj: obj}

	const off = 12345
	buf := make([]byte, 777)
	n, err := store.ReadAtSegment(context.Background(), obj.Segments[0], off, buf)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAtSegment: %v", err)
	}
	require.Equal(t, plaintext[off:off+n], buf[:n], "range bytes mismatch at offset %d", off)
}

func makeChunkedTestBody(size int) []byte {
	body := make([]byte, size)
	// xorshift64: produces pseudo-random bytes that zstd cannot compress,
	// unlike the old cyclic (i*31)%251 pattern (period 251) that was
	// trivially compressed. Tests that need compressible data use bytes.Repeat.
	state := uint64(0xcafe12345678abcd)
	for i := range body {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		body[i] = byte(state)
	}
	return body
}
