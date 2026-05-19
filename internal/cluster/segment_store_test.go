package cluster

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestClusterSegmentStore_OpenSegment_LocalDataShard(t *testing.T) {
	b, bucket, key, body := putChunkedTestObject(t)
	obj, err := b.HeadObject(context.Background(), bucket, key)
	require.NoError(t, err)
	require.NotEmpty(t, obj.Segments)

	store := &clusterSegmentStore{b: b, bucket: bucket, key: key, obj: obj}
	rc, err := store.OpenSegment(context.Background(), obj.Segments[0])
	require.NoError(t, err)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, body[:storage.DefaultChunkSize], got)
}

func TestGetObject_ChunkedRoutesToSegmentStore(t *testing.T) {
	b, bucket, key, body := putChunkedTestObject(t)

	rc, obj, err := b.GetObject(context.Background(), bucket, key)
	require.NoError(t, err)
	require.NotNil(t, obj)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, body, got)
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

func TestReadAt_Chunked_CrossSegmentBoundary(t *testing.T) {
	b, bucket, key, body := putChunkedTestObject(t)
	offset := int64(storage.DefaultChunkSize - 3)
	buf := make([]byte, 8)

	n, err := b.ReadAt(context.Background(), bucket, key, offset, buf)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.Equal(t, body[offset:offset+int64(len(buf))], buf)
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
	b.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-a": {ID: "group-a", PeerIDs: []string{"self", "self", "self"}},
	}})

	const (
		bucket = "chunked-bucket"
		key    = "large-object"
	)
	body := makeChunkedTestBody(storage.DefaultChunkSize + 4096)

	require.NoError(t, b.CreateBucket(context.Background(), bucket))
	_, err := b.PutObject(context.Background(), bucket, key, bytes.NewReader(body), "application/octet-stream")
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
