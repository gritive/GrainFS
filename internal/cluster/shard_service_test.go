package cluster

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/transport"
)

func TestShardService_LocalWriteAndRead(t *testing.T) {
	dir := t.TempDir()
	tr := transport.NewQUICTransport()
	svc := NewShardService(dir, tr)

	// Verify shards directory created
	_, err := os.Stat(filepath.Join(dir, "shards"))
	require.NoError(t, err)

	// Write a shard locally via handleRPC
	handler := svc.HandleRPC()
	require.NotNil(t, handler)

	// Direct local write
	shardDir := filepath.Join(dir, "shards", "test-bucket", "test-key")
	require.NoError(t, os.MkdirAll(shardDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(shardDir, "shard_0"), []byte("hello shard"), 0o644))

	// Read it back
	data, err := os.ReadFile(filepath.Join(shardDir, "shard_0"))
	require.NoError(t, err)
	assert.Equal(t, "hello shard", string(data))
}

func TestShardService_RPCWriteReadDelete(t *testing.T) {
	ctx := context.Background()

	// Set up two QUIC transports to simulate two nodes
	tr1 := transport.NewQUICTransport()
	tr2 := transport.NewQUICTransport()
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()

	// Connect them
	require.NoError(t, tr1.Connect(ctx, tr2.LocalAddr()))

	dir1 := t.TempDir()
	dir2 := t.TempDir()

	svc1 := NewShardService(dir1, tr1)
	svc2 := NewShardService(dir2, tr2)

	// Set tr2's stream handler to svc2's handler (simulating node2's shard server)
	tr2.SetStreamHandler(svc2.HandleRPC())

	// Node1 writes a shard to Node2
	err := svc1.WriteShard(ctx, tr2.LocalAddr(), "mybucket", "mykey", 0, []byte("shard-data-0"))
	require.NoError(t, err)

	// Verify shard landed on Node2's disk
	shardPath := filepath.Join(dir2, "shards", "mybucket", "mykey", "shard_0")
	data, err := os.ReadFile(shardPath)
	require.NoError(t, err)
	assert.Equal(t, "shard-data-0", string(data))

	// Node1 reads the shard back from Node2
	got, err := svc1.ReadShard(ctx, tr2.LocalAddr(), "mybucket", "mykey", 0)
	require.NoError(t, err)
	assert.Equal(t, "shard-data-0", string(got))

	// Node1 deletes the shard on Node2
	err = svc1.DeleteShards(ctx, tr2.LocalAddr(), "mybucket", "mykey")
	require.NoError(t, err)

	// Verify shard is gone on Node2
	_, err = os.ReadFile(shardPath)
	assert.True(t, os.IsNotExist(err))
}
