package cluster

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
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

	_, err := writer.writeShardReaders(context.Background(), plan, sp, func(idx int) (io.Reader, error) {
		return strings.NewReader("shard"), nil
	}, "test")
	require.ErrorIs(t, err, writeErr)
	require.Equal(t, []string{"bucket/object/v1"}, shards.deleteLocalCalls)
	require.Empty(t, shards.deleteRemoteCalls)
}

func TestECObjectWriter_WriteSingleLocalReaderAddsHeaderAndHash(t *testing.T) {
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
	sp := &spooledObject{Size: 5}

	result, err := writer.writeSingleLocalReader(context.Background(), plan, sp, strings.NewReader("hello"), "test", md5.New())
	require.NoError(t, err)

	require.Len(t, shards.localWrites, 1)
	gotBody := shards.localWrites[0].body
	require.Len(t, gotBody, shardHeaderSize+len("hello"))
	gotSize, _, err := decodeShardHeader(gotBody[:shardHeaderSize])
	require.NoError(t, err)
	require.Equal(t, int64(5), gotSize)
	require.Equal(t, "hello", string(gotBody[shardHeaderSize:]))
	require.Equal(t, "5d41402abc4b2a76b9719d911017c592", result.ETag)
	require.Equal(t, result.ETag, sp.ETag)
	require.Equal(t, "object/v1", result.ShardKey)
	require.Equal(t, uint8(1), result.ECData)
	require.Equal(t, uint8(0), result.ECParity)
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

func TestECObjectWriter_WriteMemoryShardsUsesBufferedRemoteShardWrites(t *testing.T) {
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

	result, err := writer.writeMemoryShards(context.Background(), plan, sp)
	require.NoError(t, err)

	require.Len(t, shards.bufferedWrites, 1)
	require.Empty(t, shards.streamWrites)
	require.Equal(t, "node-b", shards.bufferedWrites[0].peer)
	require.Equal(t, int64(5), result.Size)
}

func TestECObjectWriter_WriteMemoryShardsUsesBufferedLocalShardWrites(t *testing.T) {
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
		Placement:        []string{"node-a"},
		ContentType:      "text/plain",
	}

	result, err := writer.writeMemoryShards(context.Background(), plan, sp)
	require.NoError(t, err)

	require.Len(t, shards.bufferedLocalWrites, 1)
	require.Empty(t, shards.localWrites)
	require.Equal(t, "object/v1", shards.bufferedLocalWrites[0].key)
	require.Len(t, shards.bufferedLocalWrites[0].body, shardHeaderSize+len("hello"))
	gotSize, _, err := decodeShardHeader(shards.bufferedLocalWrites[0].body[:shardHeaderSize])
	require.NoError(t, err)
	require.Equal(t, int64(5), gotSize)
	require.Equal(t, []byte("hello"), shards.bufferedLocalWrites[0].body[shardHeaderSize:])
	require.Equal(t, int64(5), result.Size)
}

func TestECObjectWriter_WriteRemoteShardRecordsTraceBreakdown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "put-trace.jsonl")
	t.Setenv("GRAINFS_PUT_TRACE_FILE", path)
	reloadPutTraceSinkForTest()
	t.Cleanup(reloadPutTraceSinkForTest)

	shards := &fakeECObjectWriterShards{}
	writer := ecObjectWriter{
		selfID:        "node-a",
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

	err := writer.writeRemoteShard(ctx,
		func(idx int) (io.Reader, error) {
			require.Equal(t, 2, idx)
			return strings.NewReader("remote-shard"), nil
		},
		func(idx int) (int64, error) {
			require.Equal(t, 2, idx)
			return int64(len("remote-shard")), nil
		},
		2,
		"node-b",
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
	expected := PlacementForNodes(cfg, peers, "object/segments/"+blobID)

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
	localWrites         []fakeECObjectWriterLocalWrite
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
	data, _ := io.ReadAll(body)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.localWrites = append(f.localWrites, fakeECObjectWriterLocalWrite{
		bucket:   bucket,
		key:      key,
		shardIdx: shardIdx,
		body:     data,
	})
	return nil
}

func (f *fakeECObjectWriterShards) WriteLocalShardStreamContext(ctx context.Context, bucket, key string, shardIdx int, body io.Reader) error {
	return f.WriteLocalShardStream(bucket, key, shardIdx, body)
}

// localDataDirs returns nil so the test fake exercises the legacy spool path
// in writeSpooledShards (the C optimisation only kicks in when the shard
// service exposes drive roots).
func (f *fakeECObjectWriterShards) localDataDirs() []string { return nil }

// importLocalShardFromPath is unreachable for the fake because localDataDirs
// returns nil; surface a sentinel error to catch wiring regressions.
func (f *fakeECObjectWriterShards) importLocalShardFromPath(ctx context.Context, bucket, key string, shardIdx int, srcPath string) error {
	return fmt.Errorf("fakeECObjectWriterShards.importLocalShardFromPath called but C optimisation is disabled in tests")
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
	rawKey := make([]byte, 32)
	enc, err := encrypt.NewEncryptor(rawKey)
	require.NoError(t, err)

	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), WithEncryptor(enc))

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
