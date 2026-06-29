package cluster

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSpoolECShardsWritesPlaintext confirms that EC shard spool files contain
// raw (unencrypted) shard data after the double-encryption removal.
func TestSpoolECShardsWritesPlaintext(t *testing.T) {
	b := newTestDistributedBackendDEK(t)
	dir := t.TempDir()
	payload := bytes.Repeat([]byte("Q"), 1<<20) // 1 MiB
	sp, err := b.spoolPutObject(context.Background(), "bkt", bytes.NewReader(payload), false)
	require.NoError(t, err)
	defer sp.Cleanup()

	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	shards, err := spoolECShards(context.Background(), cfg, dir, sp)
	require.NoError(t, err)
	defer shards.Cleanup()

	// Data shard 0 must be a raw file readable without decryption.
	// Encrypted spool records start with a 12-byte header; a plain shard
	// file starts directly with shard payload bytes.
	f, err := os.Open(shards.paths[0])
	require.NoError(t, err)
	defer f.Close()
	buf := make([]byte, 256)
	n, err := io.ReadAtLeast(f, buf, 1)
	require.NoError(t, err)
	require.Greater(t, n, 0)

	raw, err := os.ReadFile(shards.paths[0])
	require.NoError(t, err)
	require.True(t, bytes.Contains(raw, bytes.Repeat([]byte("Q"), 4096)), "data shard 0 must contain plaintext payload bytes")
}

// TestSpoolECShardsZeroSizeIsClean verifies the zero-size path in spoolECShards
// creates empty shard files without the now-removed domain tracking.
func TestSpoolECShardsZeroSizeIsClean(t *testing.T) {
	b := newTestDistributedBackendDEK(t)
	dir := t.TempDir()
	// Create a 0-byte spooled object directly — spoolPutObject will succeed.
	sp, err := b.spoolPutObject(context.Background(), "bkt", bytes.NewReader(nil), false)
	require.NoError(t, err)
	defer sp.Cleanup()
	require.Equal(t, int64(0), sp.Size)

	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	shards, err := spoolECShards(context.Background(), cfg, dir, sp)
	require.NoError(t, err)
	defer shards.Cleanup()

	// All 6 shard files must exist and be empty.
	require.Len(t, shards.paths, cfg.NumShards())
	for i, path := range shards.paths {
		info, err := os.Stat(path)
		require.NoError(t, err, "shard %d missing", i)
		require.Equal(t, int64(0), info.Size(), "shard %d must be empty", i)
	}
}
