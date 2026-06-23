package cluster

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/gritive/GrainFS/internal/hrw"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestECObjectWriter_CleansWrittenShardsOnWriteFailure(t *testing.T) {
	writeErr := errors.New("remote write failed")
	shards := &fakeECObjectWriterShards{
		writeShardErr: map[string]error{"node-b": writeErr},
	}
	writer := ecObjectWriter{
		selfID:        "node-a",
		shards:        shards,
		writeAttempts: 1,
	}

	plan := ecObjectWritePlan{
		Bucket:           "bucket",
		Key:              "object",
		VersionID:        "v1",
		PlacementGroupID: "group-1",
		Config:           ECConfig{DataShards: 1, ParityShards: 1},
		Placement:        []string{"node-a", "node-b"},
		ContentType:      "application/octet-stream",
	}
	sp := &spooledObject{Size: 11, ETag: "etag"}

	_, err := writer.writeShardReadersWithSize(context.Background(), plan, sp, func(idx int) (io.Reader, error) {
		return strings.NewReader("shard"), nil
	}, nil, "test")
	require.ErrorIs(t, err, writeErr)
	require.Equal(t, []string{"bucket/object/v1"}, shards.deleteLocalCalls)
	require.Empty(t, shards.deleteRemoteCalls)
}

func TestECObjectWriter_WriteSpooledShardsMaterializesAndWritesBufferedRemote(t *testing.T) {
	shards := &fakeECObjectWriterShards{}
	writer := ecObjectWriter{
		selfID: "node-a",
		shards: shards,
	}
	dir := t.TempDir()
	spoolPath := filepath.Join(dir, "object")
	require.NoError(t, os.WriteFile(spoolPath, []byte("hello"), 0o600))
	sp := &spooledObject{Path: spoolPath, Size: 5, ETag: "etag"}
	plan := ecObjectWritePlan{
		Bucket:           "bucket",
		Key:              "object",
		VersionID:        "v1",
		PlacementGroupID: "group-1",
		Config:           ECConfig{DataShards: 1, ParityShards: 0},
		Placement:        []string{"node-b"},
		ContentType:      "text/plain",
	}

	result, err := writer.writeSpooledShards(context.Background(), plan, dir, sp)
	require.NoError(t, err)

	require.Len(t, shards.bufferedWrites, 1)
	write := shards.bufferedWrites[0]
	require.Equal(t, "node-b", write.peer)
	require.Equal(t, "object/v1", write.key)
	require.Equal(t, int64(5), result.Size)
	require.Equal(t, "etag", result.ETag)
	require.Equal(t, uint8(1), result.ECData)
}

func TestECObjectWriter_WriteRemoteShardRecordsTraceBreakdown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "put-trace.jsonl")
	t.Setenv("GRAINFS_PUT_TRACE_FILE", path)
	reloadPutTraceSinkForTest()
	t.Cleanup(reloadPutTraceSinkForTest)

	shards := &fakeECObjectWriterShards{}
	endpoint := remoteShardEndpoint{
		node:          "node-b",
		shards:        shards,
		writeAttempts: 1,
	}
	ctx := ContextWithPutTrace(context.Background(), PutTraceRequest{
		Bucket:      "bucket",
		Key:         "object",
		GroupID:     "group-1",
		Ingress:     PutTraceIngressLocalLeader,
		SizeClass:   PutTraceSizeSmall,
		ForwardMode: PutTraceForwardNone,
	})

	err := endpoint.writeRemoteShard(ctx,
		func(idx int) (io.Reader, error) {
			require.Equal(t, 2, idx)
			return strings.NewReader("remote-shard"), nil
		},
		func(idx int) (int64, error) {
			require.Equal(t, 2, idx)
			return int64(len("remote-shard")), nil
		},
		2,
		"bucket",
		"object/v1",
	)
	require.NoError(t, err)

	events := readECObjectWriterTraceEvents(t, path)
	requireECObjectWriterTraceStage(t, events, PutTraceStageShardWriteRemoteOpen)
	requireECObjectWriterTraceStage(t, events, PutTraceStageShardWriteRemoteBuffer)
	requireECObjectWriterTraceStage(t, events, PutTraceStageShardWriteRemoteRPC)
}

func TestECObjectWriter_WriteDataShardsComputesObjectFacts(t *testing.T) {
	shards := &fakeECObjectWriterShards{}
	writer := ecObjectWriter{
		selfID: "node-a",
		shards: shards,
	}
	plan := ecObjectWritePlan{
		Bucket:           "bucket",
		Key:              "object",
		VersionID:        "v1",
		PlacementGroupID: "group-1",
		Config:           ECConfig{DataShards: 1, ParityShards: 0},
		Placement:        []string{"node-a"},
		ContentType:      "text/plain",
	}

	result, err := writer.writeDataShards(context.Background(), plan, []byte("hello"))
	require.NoError(t, err)

	require.Len(t, shards.bufferedLocalWrites, 1)
	require.Equal(t, int64(5), result.Size)
	require.Equal(t, "5d41402abc4b2a76b9719d911017c592", result.ETag)
	require.Equal(t, "object/v1", result.ShardKey)
	require.Equal(t, []string{"node-a"}, result.Placement)
}

func TestECObjectWriter_WriteDataShardsAllocBytesBounded(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789abcdef"), 128*1024)
	cfg := ECConfig{DataShards: 2, ParityShards: 2}
	placement := []string{"node-a", "node-b", "node-c", "node-d"}

	run := func(t testing.TB) {
		t.Helper()
		shards := &fakeECObjectWriterShards{}
		writer := ecObjectWriter{
			selfID: "not-a-placement-node",
			shards: shards,
		}
		_, err := writer.writeDataShards(context.Background(), ecObjectWritePlan{
			Bucket:    "bucket",
			Key:       "object",
			Config:    cfg,
			Placement: placement,
		}, data)
		require.NoError(t, err)
		require.Len(t, shards.streamWrites, cfg.NumShards())
	}

	run(t)
	allocedBytes := int64(allocBytesPerRunForTest(t, 3, func() error {
		run(t)
		return nil
	}))
	t.Logf("writeDataShards alloc bytes: %d", allocedBytes)
	require.LessOrEqual(t, allocedBytes, int64(3*1024*1024))
}

func TestECObjectWriter_WriteOneSegmentRotatesPlacementBySegmentShardKey(t *testing.T) {
	shards := &fakeECObjectWriterShards{}
	writer := ecObjectWriter{
		selfID: "not-a-placement-node",
		shards: shards,
	}
	cfg := ECConfig{DataShards: 2, ParityShards: 2}
	peers := []string{"node-a", "node-b", "node-c", "node-d"}
	blobID := "blob-1"
	expected := hrw.PlaceShards("object/segments/"+blobID, peers, nil, cfg.NumShards())

	rec, _, _, err := writer.writeOneSegment(context.Background(), writeSegmentInput{
		Bucket:        "bucket",
		Key:           "object",
		SegmentBlobID: blobID,
		SegmentIdx:    0,
		Group:         ShardGroupEntry{ID: "group-1", PeerIDs: peers},
		Cfg:           cfg,
		Data:          []byte("segment payload"),
	})

	require.NoError(t, err)
	require.Equal(t, expected, rec.Nodes)
	require.Len(t, shards.bufferedWrites, cfg.NumShards())
	for _, write := range shards.bufferedWrites {
		require.Contains(t, expected, write.peer)
	}
}

// TestWriteOneSegment_HRWPlacement pins the chunked segment-write path onto
// weighted HRW. The recorded placement (PlacementRecord.Nodes) must equal
// hrw.PlaceShards(placementKey, group.PeerIDs, weights, NumShards) — NOT the
// legacy FNV-32 modulo order (PlacementForNodes). With nil weights / no node
// stats this is unweighted HRW (the all-stale fallback the object_put.go path
// already uses).
func TestWriteOneSegment_HRWPlacement(t *testing.T) {
	shards := &fakeECObjectWriterShards{}
	writer := ecObjectWriter{
		selfID: "not-a-placement-node",
		shards: shards,
	}
	cfg := ECConfig{DataShards: 2, ParityShards: 2}
	peers := []string{"node-a", "node-b", "node-c", "node-d"}
	blobID := "blob-1"
	placementKey := "object/segments/" + blobID
	wantHRW := hrw.PlaceShards(placementKey, peers, nil, cfg.NumShards())

	rec, _, _, err := writer.writeOneSegment(context.Background(), writeSegmentInput{
		Bucket:        "bucket",
		Key:           "object",
		SegmentBlobID: blobID,
		SegmentIdx:    0,
		Group:         ShardGroupEntry{ID: "group-1", PeerIDs: peers},
		Cfg:           cfg,
		Data:          []byte("segment payload"),
	})
	require.NoError(t, err)
	require.Equal(t, wantHRW, rec.Nodes,
		"segment placement must use weighted HRW, not FNV-32 modulo")
	require.Len(t, rec.Nodes, cfg.NumShards())

	// All shards land on distinct nodes when group N == k+m.
	seen := make(map[string]struct{}, len(rec.Nodes))
	for _, n := range rec.Nodes {
		_, dup := seen[n]
		require.Falsef(t, dup, "shard placed twice on %s", n)
		seen[n] = struct{}{}
	}
}

// TestWriteOneSegment_HRWPlacement_Weighted pins the weighted variant: when a
// per-peer weight slice is supplied (mirroring b.nodeStatsStore disk-avail) the
// recorded placement must equal the weighted HRW order.
func TestWriteOneSegment_HRWPlacement_Weighted(t *testing.T) {
	shards := &fakeECObjectWriterShards{}
	writer := ecObjectWriter{
		selfID: "not-a-placement-node",
		shards: shards,
	}
	cfg := ECConfig{DataShards: 2, ParityShards: 2}
	peers := []string{"node-a", "node-b", "node-c", "node-d"}
	weights := []float64{100, 200, 50, 400}
	blobID := "blob-w"
	placementKey := "object/segments/" + blobID
	wantHRW := hrw.PlaceShards(placementKey, peers, weights, cfg.NumShards())

	rec, _, _, err := writer.writeOneSegment(context.Background(), writeSegmentInput{
		Bucket:          "bucket",
		Key:             "object",
		SegmentBlobID:   blobID,
		SegmentIdx:      0,
		Group:           ShardGroupEntry{ID: "group-1", PeerIDs: peers},
		Cfg:             cfg,
		Data:            []byte("segment payload"),
		Weights:         weights,
		WeightedEnabled: true,
	})
	require.NoError(t, err)
	require.Equal(t, wantHRW, rec.Nodes)
}

func readECObjectWriterTraceEvents(t *testing.T, path string) []PutTraceEvent {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	var out []PutTraceEvent
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var ev PutTraceEvent
		require.NoError(t, json.Unmarshal(sc.Bytes(), &ev))
		out = append(out, ev)
	}
	require.NoError(t, sc.Err())
	require.NotEmpty(t, out)
	return out
}

func requireECObjectWriterTraceStage(t *testing.T, events []PutTraceEvent, stage PutTraceStage) {
	t.Helper()
	for _, ev := range events {
		if ev.Stage == stage {
			return
		}
	}
	require.Failf(t, "missing trace stage", "stage %s not found in %#v", stage, events)
}

type fakeECObjectWriterShards struct {
	mu                  sync.Mutex
	writeShardErr       map[string]error
	bufferedLocalWrites []fakeECObjectWriterLocalWrite
	bufferedWrites      []fakeECObjectWriterBufferedWrite
	streamWrites        []fakeECObjectWriterStreamWrite
	deleteLocalCalls    []string
	deleteRemoteCalls   []string
}

type fakeECObjectWriterLocalWrite struct {
	bucket   string
	key      string
	shardIdx int
	body     []byte
}

type fakeECObjectWriterBufferedWrite struct {
	peer     string
	bucket   string
	key      string
	shardIdx int
	body     []byte
}

type fakeECObjectWriterStreamWrite struct {
	peer     string
	bucket   string
	key      string
	shardIdx int
}

func (f *fakeECObjectWriterShards) WriteLocalShardStream(bucket, key string, shardIdx int, body io.Reader) error {
	// Drain the body so the writer side completes; no surviving test inspects the
	// non-buffered local writes (the readers lived in the removed fast-path tests).
	_, _ = io.ReadAll(body)
	return nil
}

func (f *fakeECObjectWriterShards) WriteLocalShardStreamContext(ctx context.Context, bucket, key string, shardIdx int, body io.Reader) error {
	return f.WriteLocalShardStream(bucket, key, shardIdx, body)
}

func (f *fakeECObjectWriterShards) WriteLocalShard(bucket, key string, shardIdx int, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bufferedLocalWrites = append(f.bufferedLocalWrites, fakeECObjectWriterLocalWrite{
		bucket:   bucket,
		key:      key,
		shardIdx: shardIdx,
		body:     append([]byte(nil), data...),
	})
	return nil
}

func (f *fakeECObjectWriterShards) WriteLocalShardContext(ctx context.Context, bucket, key string, shardIdx int, data []byte) error {
	return f.WriteLocalShard(bucket, key, shardIdx, data)
}

func (f *fakeECObjectWriterShards) WriteShard(ctx context.Context, peer, bucket, key string, shardIdx int, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bufferedWrites = append(f.bufferedWrites, fakeECObjectWriterBufferedWrite{
		peer:     peer,
		bucket:   bucket,
		key:      key,
		shardIdx: shardIdx,
		body:     append([]byte(nil), data...),
	})
	if err := f.writeShardErr[peer]; err != nil {
		return err
	}
	return nil
}

func (f *fakeECObjectWriterShards) WriteShardStream(ctx context.Context, peer, bucket, key string, shardIdx int, body io.Reader) error {
	_, _ = io.Copy(io.Discard, body)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.streamWrites = append(f.streamWrites, fakeECObjectWriterStreamWrite{
		peer:     peer,
		bucket:   bucket,
		key:      key,
		shardIdx: shardIdx,
	})
	if err := f.writeShardErr[peer]; err != nil {
		return err
	}
	return nil
}

func (f *fakeECObjectWriterShards) DeleteLocalShards(bucket, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteLocalCalls = append(f.deleteLocalCalls, bucket+"/"+key)
	return nil
}

func (f *fakeECObjectWriterShards) DeleteShards(ctx context.Context, peer, bucket, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteRemoteCalls = append(f.deleteRemoteCalls, peer+"/"+bucket+"/"+key)
	return nil
}

// Read-side methods satisfy the unified ecShardStore interface. The EC writer
// never invokes them; they are unreachable stubs.
func (f *fakeECObjectWriterShards) ReadLocalShard(string, string, int) ([]byte, error) {
	panic("ReadLocalShard: not used by ecObjectWriter")
}

func (f *fakeECObjectWriterShards) OpenLocalShard(string, string, int) (io.ReadCloser, error) {
	panic("OpenLocalShard: not used by ecObjectWriter")
}

func (f *fakeECObjectWriterShards) ReadLocalShardAt(string, string, int, int64, []byte) (int, error) {
	panic("ReadLocalShardAt: not used by ecObjectWriter")
}

func (f *fakeECObjectWriterShards) ReadShard(context.Context, string, string, string, int) ([]byte, error) {
	panic("ReadShard: not used by ecObjectWriter")
}

func (f *fakeECObjectWriterShards) ReadShardStream(context.Context, string, string, string, int) (io.ReadCloser, error) {
	panic("ReadShardStream: not used by ecObjectWriter")
}

func (f *fakeECObjectWriterShards) ReadShardRange(context.Context, string, string, string, int, int64, int64) ([]byte, error) {
	panic("ReadShardRange: not used by ecObjectWriter")
}

func (f *fakeECObjectWriterShards) ReadShardRangeStream(context.Context, string, string, string, int, int64, int64) (io.ReadCloser, error) {
	panic("ReadShardRangeStream: not used by ecObjectWriter")
}

func TestEcObjectSegmentShardKey_EmptyBlobIDPreservesLegacy(t *testing.T) {
	plan := ecObjectWritePlan{Key: "k", VersionID: "v"}
	require.Equal(t, ecObjectShardKey("k", "v"), ecObjectSegmentShardKey(plan))
}

func TestEcObjectSegmentShardKey_BlobIDOverridesVersion(t *testing.T) {
	plan := ecObjectWritePlan{Key: "k", VersionID: "v", SegmentBlobID: "b1"}
	require.Equal(t, "k/segments/b1", ecObjectSegmentShardKey(plan))
}

func TestEcObjectSegmentShardKey_DifferentBlobIDsDifferentKeys(t *testing.T) {
	a := ecObjectSegmentShardKey(ecObjectWritePlan{Key: "k", SegmentBlobID: "blob1"})
	b := ecObjectSegmentShardKey(ecObjectWritePlan{Key: "k", SegmentBlobID: "blob2"})
	require.NotEqual(t, a, b)
}

// TestEcObjectSegmentShardKey_AADPropagation is a regression guard: when the
// segment-scoped shardKey is plumbed through ShardService.WriteLocalShard, the
// existing inline AAD (bucket+"/"+key+"/"+shardIdx) automatically becomes
// segment-scoped — no ShardService changes needed.
func TestEcObjectSegmentShardKey_AADPropagation(t *testing.T) {
	keeper, clusterID := testDEKKeeper(t)

	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewHTTPTransport("test-cluster-psk"), WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	bucket := "bucket"
	plan := ecObjectWritePlan{Key: "obj", VersionID: "v1", SegmentBlobID: "blob1", SegmentIdx: 0}
	shardKey := ecObjectSegmentShardKey(plan)
	require.Equal(t, "obj/segments/blob1", shardKey)

	payload := []byte("segment-encrypted-payload")
	shardIdx := 2
	require.NoError(t, svc.WriteLocalShard(bucket, shardKey, shardIdx, payload))

	// Round-trip with the same shardKey must succeed.
	got, err := svc.ReadLocalShard(bucket, shardKey, shardIdx)
	require.NoError(t, err)
	assert.Equal(t, payload, got)

	// Reading the same on-disk bytes with the legacy (non-segment) shardKey
	// must fail — confirming the AAD is segment-scoped via the shardKey.
	legacyKey := ecObjectShardKey(plan.Key, plan.VersionID)
	src := filepath.Join(dir, "shards", bucket, shardKey, "shard_2")
	raw, err := os.ReadFile(src)
	require.NoError(t, err)
	dstDir := filepath.Join(dir, "shards", bucket, legacyKey)
	require.NoError(t, os.MkdirAll(dstDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dstDir, "shard_2"), raw, 0o600))

	_, err = svc.ReadLocalShard(bucket, legacyKey, shardIdx)
	require.Error(t, err, "decryption with legacy shardKey must fail — AAD must be segment-scoped")
}
