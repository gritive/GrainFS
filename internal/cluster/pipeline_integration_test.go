package cluster_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/cluster/putpipeline"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
)

// TestDistributedBackend_PutPipelineRoundTrip is the load-bearing
// round-trip test for Phase 5.4: it verifies that PutObjectWithRequest
// dispatches to the actor pipeline, shards land on disk under the path
// layout the ShardService reader expects, and GetObject decrypts them
// correctly. This test exercises the AAD compatibility fix (cpupool.go:
// bucket+"/"+shardKey+"/"+shardIdx instead of the transient put-ID form).
func TestDistributedBackend_PutPipelineRoundTrip(t *testing.T) {
	b := cluster.NewTestDistributedBackend(t)
	ctx := context.Background()

	// Wire an encryptor — the pipeline dispatch guard requires it.
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0xAB}, 32))
	require.NoError(t, err)
	b.SetShardService(cluster.NewShardService(b.Root(), nil, cluster.WithEncryptor(enc)), []string{b.SelfAddr()})

	// The pipeline must write shards to the same root as the ShardService.
	// ShardService resolves dataDirs as filepath.Join(dataDir, "shards").
	shardsDir := filepath.Join(b.Root(), "shards")
	pipeline := putpipeline.New(putpipeline.Config{
		DataDirs:  []string{shardsDir},
		Encryptor: enc,
		ECConfig:  cluster.ECConfig{DataShards: 1, ParityShards: 0},
	})
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pipeline.Shutdown(shutdownCtx)
	})
	b.SetPutPipeline(pipeline)

	require.NoError(t, b.CreateBucket(ctx, "bucket"))

	payload := []byte("hello from the actor pipeline")
	size := int64(len(payload))
	obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:      "bucket",
		Key:         "actor.txt",
		Body:        bytes.NewReader(payload),
		SizeHint:    &size,
		ContentType: "text/plain",
	})
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), obj.Size)
	require.Equal(t, "text/plain", obj.ContentType)
	require.NotEmpty(t, obj.ETag)
	want := md5.Sum(payload)
	require.Equal(t, hex.EncodeToString(want[:]), obj.ETag)

	// GET must decrypt and return the exact bytes PUT via the pipeline.
	rc, gotObj, err := b.GetObject(ctx, "bucket", "actor.txt")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, payload, got, "decrypted GET body must match original payload")
	require.Equal(t, obj.ETag, gotObj.ETag)
	require.Equal(t, obj.Size, gotObj.Size)
}
