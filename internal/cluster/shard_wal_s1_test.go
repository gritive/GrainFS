package cluster

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/datawal"
)

// countShardWALRecords replays the shard data WAL and counts OpShardPut records.
func countShardWALRecords(t *testing.T, shardDir string, keeper *encrypt.DEKKeeper, clusterID []byte) int {
	t.Helper()
	sealer := storage.NewDEKKeeperAdapter(keeper, clusterID)
	n := 0
	err := datawal.Replay(context.Background(), filepath.Join(shardDir, "datawal"), 0, sealer, datawal.NamespaceShard, func(rec datawal.Record) error {
		if rec.Op == datawal.OpShardPut {
			n++
		}
		return nil
	})
	require.NoError(t, err)
	return n
}

// newS1ShardSvc builds a ShardService + backend with an on-disk WAL and the given
// EC config. It returns the shard dir + the DEK keeper/clusterID so the WAL can be
// replayed for record inspection (threaded out instead of a production accessor).
// extraOpts lets a caller wire WithNoRedundancy.
func newS1ShardSvc(t *testing.T, ec ECConfig, nodes []string, extraOpts ...ShardServiceOption) (*DistributedBackend, string, *encrypt.DEKKeeper, []byte) {
	t.Helper()
	shardDir := t.TempDir()
	keeper, clusterID := testDEKKeeper(t)
	dwal, err := datawal.Open(filepath.Join(shardDir, "datawal"), storage.NewDEKKeeperAdapter(keeper, clusterID), datawal.NamespaceShard)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dwal.Close() })
	opts := append([]ShardServiceOption{WithShardDEKKeeper(keeper, clusterID), WithDataWAL(dwal)}, extraOpts...)
	svc := NewShardService(shardDir, nil, opts...)
	backend := NewSingletonBackendForTest(t)
	backend.shardSvc = svc
	backend.selfAddr = "self"
	backend.allNodes = nodes
	backend.SetECConfig(ec)
	require.NoError(t, backend.CreateBucket(context.Background(), "b"))
	return backend, shardDir, keeper, clusterID
}

// TestAppendShardDataWAL_LargeRedundant_SkipsWAL proves S1: a large shard with EC
// redundancy (ParityShards>0) writes NO OpShardPut record — durability/repair come
// from EC + the background scrubber (S0), not the WAL.
//
// RED before S1: PutObject writes one metadata-only OpShardPut per large shard.
// Mutation: restore the unconditional metadata-only Append+Flush → count > 0 → RED.
func TestAppendShardDataWAL_LargeRedundant_SkipsWAL(t *testing.T) {
	backend, shardDir, keeper, clusterID := newS1ShardSvc(t, ECConfig{DataShards: 2, ParityShards: 1}, []string{"self", "self", "self"})

	large := bytes.Repeat([]byte("s1-large-redundant-"), 1<<17) // > 1MiB per shard
	_, err := backend.PutObject(context.Background(), "b", "obj-large", bytes.NewReader(large), "application/octet-stream")
	require.NoError(t, err)

	require.Equal(t, 0, countShardWALRecords(t, shardDir, keeper, clusterID),
		"large redundant shards must write no OpShardPut WAL record")

	// The object is still readable (shard file written, just no WAL record).
	rc, _, err := backend.GetObject(context.Background(), "b", "obj-large")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, large, got)
}

// TestAppendShardDataWAL_Small_KeepsInlineWAL proves small shards are unchanged:
// the WAL still stores inline-payload OpShardPut records (their sole durability).
func TestAppendShardDataWAL_Small_KeepsInlineWAL(t *testing.T) {
	backend, shardDir, keeper, clusterID := newS1ShardSvc(t, ECConfig{DataShards: 2, ParityShards: 1}, []string{"self", "self", "self"})

	small := []byte("tiny") // << 1MiB per shard
	_, err := backend.PutObject(context.Background(), "b", "obj-small", bytes.NewReader(small), "application/octet-stream")
	require.NoError(t, err)

	require.Greater(t, countShardWALRecords(t, shardDir, keeper, clusterID), 0,
		"small shards must still write inline-payload WAL records")
}

// TestAppendShardDataWAL_LargeNoRedundancy_KeepsRecord proves the no-redundancy
// large path is untouched by S1 (it still writes a metadata-only record; S2 will
// replace it with a direct fsync). WithNoRedundancy(true) + 1+0.
func TestAppendShardDataWAL_LargeNoRedundancy_KeepsRecord(t *testing.T) {
	backend, shardDir, keeper, clusterID := newS1ShardSvc(t,
		ECConfig{DataShards: 1, ParityShards: 0}, []string{"self"},
		WithNoRedundancy(func() bool { return true }))

	large := bytes.Repeat([]byte("s1-large-noredund-"), 1<<17)
	_, err := backend.PutObject(context.Background(), "b", "obj-nr", bytes.NewReader(large), "application/octet-stream")
	require.NoError(t, err)

	require.Greater(t, countShardWALRecords(t, shardDir, keeper, clusterID), 0,
		"large no-redundancy shards must still write a metadata-only WAL record (S2 territory)")
}
