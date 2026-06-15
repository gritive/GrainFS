package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// newSingleNode1Plus0ChunkCapable builds a single-node EC 1+0 backend that is
// chunk-capable (shardGroup wired, small chunk threshold) so a small body
// exercises the segment/chunked path without a 64 MiB object.
func newSingleNode1Plus0ChunkCapable(t *testing.T) *DistributedBackend {
	t.Helper()
	backend := NewSingletonBackendForTest(t)
	const selfAddr = "self"
	keeper, clusterID := testDEKKeeper(t)
	backend.shardSvc = NewShardService(t.TempDir(), nil, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	backend.selfAddr = selfAddr
	backend.allNodes = []string{selfAddr}
	backend.SetECConfig(ECConfig{DataShards: 1, ParityShards: 0})
	backend.chunkedPutChunkSize = 1 << 10 // 1 KiB chunk threshold
	backend.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-0": {ID: "group-0", PeerIDs: []string{selfAddr}},
	}})
	return backend
}

// TestSinglePutPath_KnownSizeLarge1Plus0_Chunks proves the fix: a known-size
// (SizeHint) object larger than the chunk threshold on single-node 1+0 routes
// through the chunked path (obj.Segments non-empty) instead of being written as
// one whole-object shard. RED before the fix: the known-size single-local fast
// path intercepts it, writes a single shard, and obj.Segments is empty.
func TestSinglePutPath_KnownSizeLarge1Plus0_Chunks(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))

	body := makeChunkedTestBody(8 << 10) // 8 KiB > 1 KiB chunk threshold
	size := int64(len(body))
	obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket: "b", Key: "big", Body: bytes.NewReader(body),
		ContentType: "application/octet-stream", SizeHint: &size,
	})
	require.NoError(t, err)
	require.NotEmpty(t, obj.Segments, "known-size large object must chunk (RED: fast path writes one shard, no segments)")

	rc, _, gerr := b.GetObject(ctx, "b", "big")
	require.NoError(t, gerr)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, body, got, "chunked object must round-trip byte-identical")
}

// TestSinglePutPath_SmallObject_AlsoChunks proves the single path is
// size-independent: a tiny simple PUT on a shardGroup-wired backend ALSO chunks
// (obj.Segments non-empty), not just large objects, and round-trips.
func TestSinglePutPath_SmallObject_AlsoChunks(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))

	body := []byte("tiny-known-size")
	size := int64(len(body))
	obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket: "b", Key: "small", Body: bytes.NewReader(body),
		ContentType: "application/octet-stream", SizeHint: &size,
	})
	require.NoError(t, err)
	require.NotEmpty(t, obj.Segments, "small simple PUT takes the same chunked path as large (size-independent)")

	rc, _, gerr := b.GetObject(ctx, "b", "small")
	require.NoError(t, gerr)
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	require.Equal(t, body, got)
}

// TestSinglePutPath_EmptyObject_Chunks proves a 0-byte object now takes the same
// chunked path (one empty segment) and round-trips empty — no exemption.
func TestSinglePutPath_EmptyObject_Chunks(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))

	size := int64(0)
	obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket: "b", Key: "empty", Body: bytes.NewReader(nil),
		ContentType: "application/octet-stream", SizeHint: &size,
	})
	require.NoError(t, err, "empty object must survive the chunked path (1 empty segment)")
	require.NotEmpty(t, obj.Segments, "empty object now takes the same chunked path (1 empty segment)")

	rc, gobj, gerr := b.GetObject(ctx, "b", "empty")
	require.NoError(t, gerr)
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	require.Empty(t, got)
	require.Equal(t, int64(0), gobj.Size)
}

// TestSinglePutPath_InternalBucket_ChunksWithXXH3 proves internal buckets now
// take the same chunked path (no exemption), and the chunked SegmentWriter
// preserves their xxhash3 ETag (16-hex, the EC-rewrap corruption oracle) rather
// than the S3 MD5 — so rewrap verification still holds.
func TestSinglePutPath_InternalBucket_ChunksWithXXH3(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const internalBkt = "__grainfs_receipts"
	require.NoError(t, b.CreateBucket(ctx, internalBkt))

	body := []byte("internal-object-body-now-chunked")
	size := int64(len(body))
	obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket: internalBkt, Key: "r", Body: bytes.NewReader(body),
		ContentType: "application/octet-stream", SizeHint: &size,
	})
	require.NoError(t, err)
	require.NotEmpty(t, obj.Segments, "internal bucket object now takes the same chunked path")
	require.Equal(t, storage.InternalETag(body), obj.ETag, "internal bucket ETag must stay xxhash3, not MD5")
	require.Len(t, obj.ETag, 16, "xxhash3 ETag is 16 hex chars (MD5 would be 32)")

	rc, _, gerr := b.GetObject(ctx, internalBkt, "r")
	require.NoError(t, gerr)
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	require.Equal(t, body, got)
}

// TestSinglePutPath_UserBucketETagIsPlaintextMD5 pins ETag parity for a normal
// (non-internal) bucket simple PUT: ETag == md5(plaintext), known-size path.
func TestSinglePutPath_UserBucketETagIsPlaintextMD5(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))

	body := []byte("user-bucket-etag-parity-body")
	size := int64(len(body))
	obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket: "b", Key: "k", Body: bytes.NewReader(body),
		ContentType: "application/octet-stream", SizeHint: &size,
	})
	require.NoError(t, err)
	sum := md5.Sum(body)
	require.Equal(t, hex.EncodeToString(sum[:]), obj.ETag, "non-internal bucket ETag must be md5(plaintext)")
}
