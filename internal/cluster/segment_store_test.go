package cluster

import (
	"bytes"
	"context"
	"io"
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

		versionHead, err := b.HeadObjectVersion(bucket, key, obj.VersionID)
		require.NoError(t, err)
		require.NotEmpty(t, versionHead.Segments)

		rc, gotObj, err := b.GetObjectVersion(bucket, key, obj.VersionID)
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

	p1, err := b.UploadPart(context.Background(), bucket, key, upload.UploadID, 1, bytes.NewReader(part1))
	require.NoError(t, err)
	p2, err := b.UploadPart(context.Background(), bucket, key, upload.UploadID, 2, bytes.NewReader(part2))
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
	require.Len(t, obj.Segments, 2)

	rc, gotObj, err := b.GetObject(ctx, bucket, key)
	require.NoError(t, err)
	require.True(t, gotObj.IsAppendable)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, []byte("hello world"), got)
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

func TestClusterSegmentStore_PlacementRecordUsesSegmentMetadata(t *testing.T) {
	store := &clusterSegmentStore{}
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
		nil, "", 0, false, "", nil, nil, nil)
	require.NoError(t, err)

	return b, bucket, key, body
}

func makeChunkedTestBody(size int) []byte {
	body := make([]byte, size)
	for i := range body {
		body[i] = byte((i * 31) % 251)
	}
	return body
}
