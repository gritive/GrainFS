package cluster

// PR1 (segment staging write): the staged-write primitive must encrypt a shard with the FINAL
// logical shard key as AAD even while writing it to a STAGING physical path, so a post-promote read
// (which decrypts with the final key) succeeds. The promote is a local rename of the staging dir to
// the final path. If the AAD were the staging key, ReadLocalShard(finalKey) would fail to decrypt —
// that is exactly what this locks against (codex plan-gate load-bearing fix #2).

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/google/uuid"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// newRealStagingCSB builds a clusterSegmentBackend wired to a REAL single-node EC
// backend (real ShardService on disk, self-only placement). Unlike newCSBWithDeps
// it leaves writeSegmentFn nil so the production writeOneSegment runs — exercising
// the real staged shard write + AAD + on-disk layout. stagingTxnID is set so PR1
// segment staging is active. The caller injects promoteStagedFn to choose whether
// the commit-time promote actually runs (real) or is suppressed (RED probe).
func newRealStagingCSB(t *testing.T, b *DistributedBackend, bucket, key, txnID string, numSegments int) *clusterSegmentBackend {
	t.Helper()
	blobIDs := make([]string, numSegments)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	return &clusterSegmentBackend{
		b:            b,
		bucket:       bucket,
		key:          key,
		versionID:    "v1",
		blobIDs:      blobIDs,
		placements:   make([]segmentPlacement, numSegments),
		chunkSize:    testChunkedPutChunkSize,
		stagingTxnID: txnID,
	}
}

func realStagingBackend(t *testing.T) (*DistributedBackend, string, string, []byte) {
	t.Helper()
	b := setupECBackend(t)
	b.chunkedPutChunkSize = testChunkedPutChunkSize
	b.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-a": {ID: "group-a", PeerIDs: []string{"self", "self", "self"}},
	}})
	const bucket, key = "staging-bucket", "staging-object"
	require.NoError(t, b.CreateBucket(context.Background(), bucket))
	body := makeChunkedTestBody(testChunkedPutChunkSize + 4096) // 2 segments
	return b, bucket, key, body
}

// TestRunChunkedPut_StagingNotPromoted_ObjectUnreadable is the dependency-direction
// driver for Task 4 (staging wiring). With staging wired, segment shards land in
// .segstaging and only the commit-time promote moves them to the final path the
// manifest references. Suppressing promote MUST therefore strand the shards at
// staging and leave the committed object unreadable. Pre-wiring (shards written
// directly to final, promote irrelevant) the object reads fine — so this test fails
// (RED) until the staging write path is wired, and passes once it is.
func TestRunChunkedPut_StagingNotPromoted_ObjectUnreadable(t *testing.T) {
	ctx := context.Background()
	b, bucket, key, body := realStagingBackend(t)
	numSegments := int((int64(len(body)) + int64(testChunkedPutChunkSize) - 1) / int64(testChunkedPutChunkSize))

	csb := newRealStagingCSB(t, b, bucket, key, "txn-suppressed", numSegments)
	csb.promoteStagedFn = func(context.Context, string, string, string, string) error {
		return nil // NO-OP: suppress the commit-time promote
	}

	_, err := runChunkedPut(ctx, csb, bytes.NewReader(body),
		bucket, key, "v1", "application/octet-stream", nil, "", 0, false, "", nil, nil, nil)
	require.NoError(t, err, "commit (manifest write) still succeeds — only promote was suppressed")

	// The committed manifest points at the final segment paths, but the shards are
	// stranded at staging, so the object cannot be reconstructed. GetObject resolves
	// the metadata lazily (no eager shard read), so the missing-shard failure
	// surfaces on the body read — assert on the combined outcome.
	rd, _, gerr := b.GetObject(ctx, bucket, key)
	if gerr == nil {
		_, rerr := io.Copy(io.Discard, rd)
		_ = rd.Close()
		gerr = rerr
	}
	require.Error(t, gerr, "suppressed promote must leave shards at staging -> object unreadable")
}

// TestRunChunkedPut_StagedPromote_RoundTripByteIdentical is the happy-path
// counterpart: with the REAL commit-time promote (promoteStagedFn left nil), a
// multi-segment chunked PUT writes its shards to staging, promotes them to their
// final paths at commit, and a subsequent GET reconstructs byte-identical data.
// This locks the staging-write + final-key-AAD + promote roundtrip end-to-end at
// the local level (the e2e covers the cluster wire path).
func TestRunChunkedPut_StagedPromote_RoundTripByteIdentical(t *testing.T) {
	ctx := context.Background()
	b, bucket, key, body := realStagingBackend(t)
	numSegments := int((int64(len(body)) + int64(testChunkedPutChunkSize) - 1) / int64(testChunkedPutChunkSize))
	require.Greater(t, numSegments, 1, "fixture must span multiple segments")

	csb := newRealStagingCSB(t, b, bucket, key, "txn-roundtrip", numSegments)
	// promoteStagedFn left nil -> the real PromoteLocalStagedShards runs.

	_, err := runChunkedPut(ctx, csb, bytes.NewReader(body),
		bucket, key, "v1", "application/octet-stream", nil, "", 0, false, "", nil, nil, nil)
	require.NoError(t, err)

	rd, _, gerr := b.GetObject(ctx, bucket, key)
	require.NoError(t, gerr)
	got, rerr := io.ReadAll(rd)
	_ = rd.Close()
	require.NoError(t, rerr, "promoted shards must be readable at their final path")
	require.Equal(t, body, got, "GET after staged promote must be byte-identical")
}

// TestNativeWriteHandler_StagedRequest_WritesStagingWithFinalAAD locks the
// RECEIVER side of the staging wire: a ShardWriteRequest carrying StagingKey must
// write the bytes to the staging physical path while sealing with Key (the FINAL
// logical key) as AAD. The proof is that a post-promote ReadLocalShard(finalKey) —
// which decrypts with the final-key AAD — returns the bytes byte-identical, and a
// pre-promote read of the final key finds nothing (the bytes were at staging). This
// is the highest-risk path (a receiver deriving the dir from Key instead of
// StagingKey would corrupt silently).
func TestNativeWriteHandler_StagedRequest_WritesStagingWithFinalAAD(t *testing.T) {
	svc, _ := newTestShardService(t)
	handler := svc.NativeWriteHandler()
	const (
		bucket     = "b"
		finalKey   = "obj/segments/blobR"
		stagingKey = ".segstaging/txnR/blobR"
	)
	data := []byte("receiver-staged-shard-payload-9876543210")

	req := transport.ShardWriteRequest{Bucket: bucket, Key: finalKey, StagingKey: stagingKey, ShardIdx: 0, Sealed: false}
	require.NoError(t, handler(req, bytes.NewReader(data)))

	// Before promote, the bytes are at staging, not at the final path.
	_, preErr := svc.ReadLocalShard(bucket, finalKey, 0)
	require.Error(t, preErr, "staged write must not land at the final path before promote")

	// Promote, then the final-key read (final-key AAD) must succeed byte-identical.
	require.NoError(t, svc.PromoteLocalStagedShards(bucket, stagingKey, finalKey))
	got, err := svc.ReadLocalShard(bucket, finalKey, 0)
	require.NoError(t, err)
	require.Equal(t, data, got, "receiver must seal with finalKey AAD so the promoted read decrypts")
}

func TestWriteLocalShardStaged_AADIsFinalKey_PromoteReadable(t *testing.T) {
	svc, _ := newTestShardService(t)
	ctx := context.Background()
	const bucket = "b"
	const finalKey = "obj/segments/blob1"       // final logical shard key (drives AAD + final path)
	const stagingKey = ".segstaging/txn1/blob1" // staging physical path (no "/segments/")
	data := []byte("segment-shard-payload-0123456789")

	// Write to the STAGING physical path, but with the FINAL key as the encryption AAD.
	require.NoError(t, svc.writeLocalShardStaged(ctx, bucket, stagingKey, finalKey, 0, data))

	// Promote: local rename of the staging shard dir(s) to the final path.
	require.NoError(t, svc.PromoteLocalStagedShards(bucket, stagingKey, finalKey))

	// Reading at the FINAL key (which decrypts with the final-key AAD) must return the bytes —
	// proving the staged write used the final key as AAD, not the staging key.
	got, err := svc.ReadLocalShard(bucket, finalKey, 0)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

// PR1 Task 5: the orphan-shard walker must WHOLESALE-skip the .segstaging/ staging area, so an
// in-flight / crashed staged segment shard is never parsed as a fake full-object orphan and deleted.
// (Reclaim of abandoned staging is PR2's age-out walker, not the orphan-shard walker.)
func TestWalkOrphanShards_SkipsSegStaging(t *testing.T) {
	b := orphanWalkerBackend(t)
	root := b.shardSvc.DataDirs()[0]
	staging := writeShardLeaf(t, root, "bkt/.segstaging/txn1/blob1", []int{0}, oldEnough)

	got := collectOrphans(t, b, map[string]bool{})
	require.NotContains(t, got, staging,
		".segstaging staging dir must be skipped, never yielded as an orphan-shard candidate")
}
