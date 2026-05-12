package cluster

import (
	"context"
	"errors"
	"io"
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

type fakeECObjectWriterShards struct {
	writeShardErr     map[string]error
	deleteLocalCalls  []string
	deleteRemoteCalls []string
}

func (f *fakeECObjectWriterShards) WriteLocalShardStream(bucket, key string, shardIdx int, body io.Reader) error {
	_, _ = io.Copy(io.Discard, body)
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
