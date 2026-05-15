package cluster

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
		RingVersion:      7,
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
		RingVersion:      7,
		ContentType:      "text/plain",
	}
	sp := &spooledObject{Size: 5}

	result, err := writer.writeSingleLocalReader(plan, sp, strings.NewReader("hello"), "test", md5.New())
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
		RingVersion:      7,
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
		RingVersion:      7,
		ContentType:      "text/plain",
	}

	result, err := writer.writeMemoryShards(context.Background(), plan, sp)
	require.NoError(t, err)

	require.Len(t, shards.bufferedWrites, 1)
	require.Empty(t, shards.streamWrites)
	require.Equal(t, "node-b", shards.bufferedWrites[0].peer)
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
		RingVersion:      7,
		ContentType:      "text/plain",
	}

	result, err := writer.writeDataShards(context.Background(), plan, []byte("hello"))
	require.NoError(t, err)

	require.Len(t, shards.localWrites, 1)
	require.Equal(t, int64(5), result.Size)
	require.Equal(t, "5d41402abc4b2a76b9719d911017c592", result.ETag)
	require.Equal(t, "object/v1", result.ShardKey)
	require.Equal(t, []string{"node-a"}, result.Placement)
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
	writeShardErr     map[string]error
	localWrites       []fakeECObjectWriterLocalWrite
	bufferedWrites    []fakeECObjectWriterBufferedWrite
	streamWrites      []fakeECObjectWriterStreamWrite
	deleteLocalCalls  []string
	deleteRemoteCalls []string
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
	f.localWrites = append(f.localWrites, fakeECObjectWriterLocalWrite{
		bucket:   bucket,
		key:      key,
		shardIdx: shardIdx,
		body:     data,
	})
	return nil
}

func (f *fakeECObjectWriterShards) WriteShard(ctx context.Context, peer, bucket, key string, shardIdx int, data []byte) error {
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
	f.deleteLocalCalls = append(f.deleteLocalCalls, bucket+"/"+key)
	return nil
}

func (f *fakeECObjectWriterShards) DeleteShards(ctx context.Context, peer, bucket, key string) error {
	f.deleteRemoteCalls = append(f.deleteRemoteCalls, peer+"/"+bucket+"/"+key)
	return nil
}
