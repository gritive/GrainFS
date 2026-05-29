package cluster

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// mustShardPath / mustShardDir unwrap the (string, error) path builders for test
// call sites that use a benign key (they never escape, so the panic is dead).
// getShardPath/getShardDir gained an escape-containment error return; these keep
// the existing tests terse without each site handling an impossible error.
func mustShardPath(s *ShardService, bucket, key string, idx int) string {
	p, err := s.getShardPath(bucket, key, idx)
	if err != nil {
		panic(err)
	}
	return p
}

func mustShardDir(s *ShardService, bucket, key string, idx int) string {
	d, err := s.getShardDir(bucket, key, idx)
	if err != nil {
		panic(err)
	}
	return d
}

// newTraversalTestShardService builds a ShardService with shard-pack disabled
// (default packThreshold 0 ⇒ shardPack nil), so writeLocalShard takes the
// per-file branch that maps the object key into a filesystem path — the
// path-traversal vector.
func newTraversalTestShardService(t *testing.T) (*ShardService, string) {
	t.Helper()
	root := t.TempDir()
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(root, nil, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	return svc, root
}

// TestGetShardPath_RejectsEscapingKey verifies the chokepoint: a key with ".."
// segments that escape {dataDir}/{bucket} is rejected by both path builders,
// while a benign key resolves under the shard root. Every read/write/replica
// consumer funnels through these, so this is the transitive guard.
func TestGetShardPath_RejectsEscapingKey(t *testing.T) {
	svc, _ := newTraversalTestShardService(t)

	_, err := svc.getShardPath("bucket", "../../escape", 0)
	require.Error(t, err, "escaping key must be rejected by getShardPath")
	_, err = svc.getShardDir("bucket", "../../escape", 0)
	require.Error(t, err, "escaping key must be rejected by getShardDir")

	p, err := svc.getShardPath("bucket", "a/b/c", 0)
	require.NoError(t, err, "benign key must be accepted")
	require.True(t, svc.ShardPathUnderDataDir("bucket", 0, p))
}

// TestReadLocalShard_RejectsKeyEscapingShardRoot is the disclosure vector via the
// shared chokepoint: a crafted key must not resolve a file outside the shard
// root (covers the peer GetShard RPC path that S3 ingress cannot reach).
func TestReadLocalShard_RejectsKeyEscapingShardRoot(t *testing.T) {
	svc, _ := newTraversalTestShardService(t)

	_, err := svc.ReadLocalShard("bucket", "../../escape", 0)
	require.Error(t, err, "ReadLocalShard must reject a key that escapes the shard root")
}

// TestWriteLocalShard_RejectsKeyEscapingShardRoot is the write vector: a crafted
// key (via peer shard RPC or a malicious S3 key) must not let a shard file
// materialize outside the data dir.
func TestWriteLocalShard_RejectsKeyEscapingShardRoot(t *testing.T) {
	svc, root := newTraversalTestShardService(t)

	err := svc.writeLocalShard(context.Background(), "bucket", "../../../escape", 0, []byte("malicious"))
	require.Error(t, err, "writeLocalShard must reject a key that escapes the shard root")

	escaped := filepath.Join(filepath.Dir(root), "escape")
	_, statErr := os.Stat(escaped)
	require.Truef(t, os.IsNotExist(statErr), "no shard dir may escape the data dir; found %s", escaped)
}
