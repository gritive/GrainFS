package cluster

import (
	"context"
	"crypto/md5"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	if !errors.Is(err, writeErr) {
		t.Fatalf("writeShardReaders error = %v, want wrapping %v", err, writeErr)
	}

	if got, want := len(shards.deleteLocalCalls), 1; got != want {
		t.Fatalf("DeleteLocalShards calls = %d, want %d", got, want)
	}
	if got, want := shards.deleteLocalCalls[0], "bucket/object/v1"; got != want {
		t.Fatalf("DeleteLocalShards key = %q, want %q", got, want)
	}
	if got := len(shards.deleteRemoteCalls); got != 0 {
		t.Fatalf("DeleteShards calls = %d, want 0 because remote write never succeeded", got)
	}
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
	if err != nil {
		t.Fatalf("writeSingleLocalReader error = %v", err)
	}

	if got, want := len(shards.localWrites), 1; got != want {
		t.Fatalf("local writes = %d, want %d", got, want)
	}
	gotBody := shards.localWrites[0].body
	if got, want := len(gotBody), shardHeaderSize+len("hello"); got != want {
		t.Fatalf("local write len = %d, want %d", got, want)
	}
	gotSize, _, err := decodeShardHeader(gotBody[:shardHeaderSize])
	if err != nil {
		t.Fatalf("decode header: %v", err)
	}
	if got, want := gotSize, int64(5); got != want {
		t.Fatalf("header size = %d, want %d", got, want)
	}
	if got, want := string(gotBody[shardHeaderSize:]), "hello"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
	if got, want := result.ETag, "5d41402abc4b2a76b9719d911017c592"; got != want {
		t.Fatalf("etag = %q, want %q", got, want)
	}
	if got, want := sp.ETag, result.ETag; got != want {
		t.Fatalf("sp ETag = %q, want result ETag %q", got, want)
	}
	if got, want := result.ShardKey, "object/v1"; got != want {
		t.Fatalf("shard key = %q, want %q", got, want)
	}
	if got, want := result.ECData, uint8(1); got != want {
		t.Fatalf("ECData = %d, want %d", got, want)
	}
	if got, want := result.ECParity, uint8(0); got != want {
		t.Fatalf("ECParity = %d, want %d", got, want)
	}
}

func TestECObjectWriter_WriteSpooledShardsMaterializesAndWritesBufferedRemote(t *testing.T) {
	shards := &fakeECObjectWriterShards{}
	writer := ecObjectWriter{
		selfID: "node-a",
		shards: shards,
	}
	dir := t.TempDir()
	spoolPath := filepath.Join(dir, "object")
	if err := os.WriteFile(spoolPath, []byte("hello"), 0o600); err != nil {
		t.Fatalf("write spool: %v", err)
	}
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
	if err != nil {
		t.Fatalf("writeSpooledShards error = %v", err)
	}

	if got, want := len(shards.bufferedWrites), 1; got != want {
		t.Fatalf("buffered remote writes = %d, want %d", got, want)
	}
	write := shards.bufferedWrites[0]
	if got, want := write.peer, "node-b"; got != want {
		t.Fatalf("peer = %q, want %q", got, want)
	}
	if got, want := write.key, "object/v1"; got != want {
		t.Fatalf("key = %q, want %q", got, want)
	}
	if got, want := result.Size, int64(5); got != want {
		t.Fatalf("result size = %d, want %d", got, want)
	}
	if got, want := result.ETag, "etag"; got != want {
		t.Fatalf("result ETag = %q, want %q", got, want)
	}
	if got, want := result.ECData, uint8(1); got != want {
		t.Fatalf("ECData = %d, want %d", got, want)
	}
}

type fakeECObjectWriterShards struct {
	writeShardErr     map[string]error
	localWrites       []fakeECObjectWriterLocalWrite
	bufferedWrites    []fakeECObjectWriterBufferedWrite
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
