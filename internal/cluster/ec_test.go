package cluster

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestECConfig_IsActive(t *testing.T) {
	cases := []struct {
		name        string
		cfg         ECConfig
		clusterSize int
		want        bool
	}{
		{"enough nodes (k+m)", ECConfig{DataShards: 4, ParityShards: 2}, 6, true},
		{"minimum cluster size (3)", ECConfig{DataShards: 4, ParityShards: 2}, 3, true},
		{"single node", ECConfig{DataShards: 4, ParityShards: 2}, 1, false},
		{"two nodes", ECConfig{DataShards: 4, ParityShards: 2}, 2, false},
		{"larger cluster", ECConfig{DataShards: 4, ParityShards: 2}, 10, true},
		{"disabled data shards", ECConfig{DataShards: 0, ParityShards: 2}, 3, false},
		{"disabled parity shards", ECConfig{DataShards: 2, ParityShards: 0}, 3, false},
		{"disabled zero config", ECConfig{}, 3, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.cfg.IsActive(tc.clusterSize))
		})
	}
}

func TestPlacement_DistinctWhenNEqualsShards(t *testing.T) {
	keys := []string{"foo", "bar", "baz/biz", "", "x", "버킷/키"}
	numNodes := 6
	shardCount := 6
	for _, k := range keys {
		seen := make(map[int]bool)
		for i := 0; i < shardCount; i++ {
			n := Placement(k, i, numNodes)
			assert.Falsef(t, seen[n], "key=%q shard=%d → dup node %d", k, i, n)
			seen[n] = true
		}
	}
}

func TestPlacementForNodes_OrderingPreserved(t *testing.T) {
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	nodes := []string{"n0", "n1", "n2", "n3", "n4", "n5"}
	placement := PlacementForNodes(cfg, nodes, "mykey")
	require.Len(t, placement, 6)
	// Each shard placement must reference a node from the input list.
	nodeSet := make(map[string]bool)
	for _, n := range nodes {
		nodeSet[n] = true
	}
	for i, p := range placement {
		assert.Truef(t, nodeSet[p], "placement[%d]=%q not in nodes", i, p)
	}
}

func TestECSplit_Reconstruct_RoundTrip(t *testing.T) {
	cases := []struct {
		name string
		size int
	}{
		{"tiny 100B", 100},
		{"1KB", 1024},
		{"1MB", 1 << 20},
		{"not-aligned", 12345},
	}
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := make([]byte, tc.size)
			_, err := rand.Read(data)
			require.NoError(t, err)

			shards, err := ECSplit(cfg, data)
			require.NoError(t, err)
			require.Len(t, shards, 6)

			// Happy path: all shards available.
			got, err := ECReconstruct(cfg, append([][]byte(nil), shards...))
			require.NoError(t, err)
			assert.Truef(t, bytes.Equal(data, got), "size %d: round-trip mismatch", tc.size)
		})
	}
}

func TestECReconstruct_MissingParityShard(t *testing.T) {
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	data := make([]byte, 1024)
	_, _ = rand.Read(data)
	shards, err := ECSplit(cfg, data)
	require.NoError(t, err)

	// Drop one parity shard (index 5). k=4 of 6 still allows reconstruction.
	shards[5] = nil
	got, err := ECReconstruct(cfg, shards)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(data, got))
}

func TestECReconstruct_MissingDataShard(t *testing.T) {
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	data := make([]byte, 8192)
	_, _ = rand.Read(data)
	shards, err := ECSplit(cfg, data)
	require.NoError(t, err)

	// Drop one data shard. Parity reconstructs it.
	shards[1] = nil
	got, err := ECReconstruct(cfg, shards)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(data, got))
}

func TestECReconstruct_TwoMissing(t *testing.T) {
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	data := make([]byte, 4096)
	_, _ = rand.Read(data)
	shards, err := ECSplit(cfg, data)
	require.NoError(t, err)

	// Drop two shards (at the EC 4+2 tolerance limit).
	shards[0] = nil
	shards[5] = nil
	got, err := ECReconstruct(cfg, shards)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(data, got))
}

func TestECReconstruct_TooManyMissing(t *testing.T) {
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	data := make([]byte, 4096)
	_, _ = rand.Read(data)
	shards, err := ECSplit(cfg, data)
	require.NoError(t, err)

	// Drop 3 shards — beyond 4+2 tolerance. Should fail.
	shards[0] = nil
	shards[1] = nil
	shards[2] = nil
	_, err = ECReconstruct(cfg, shards)
	assert.Error(t, err)
}

func TestShardHeader_Roundtrip(t *testing.T) {
	sizes := []int64{0, 1, 100, 1 << 20, 1 << 40}
	for _, s := range sizes {
		h := encodeShardHeader(s)
		got, body, err := decodeShardHeader(append(h[:], 0xab, 0xcd))
		require.NoError(t, err)
		assert.Equal(t, s, got)
		assert.Equal(t, []byte{0xab, 0xcd}, body)
	}
}

func TestShardHeader_TooSmall(t *testing.T) {
	_, _, err := decodeShardHeader([]byte{1, 2, 3})
	assert.Error(t, err)
}

func TestShardFilePath_Structure(t *testing.T) {
	got := shardFilePath("/data", "bkt", "obj/path", 3)
	assert.Equal(t, "/data/ec-shards/bkt/obj/path/shard_3", got)
}

func BenchmarkECSplit(b *testing.B) {
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	data := make([]byte, 1<<20) // 1 MiB
	_, err := rand.Read(data)
	require.NoError(b, err)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = ECSplit(cfg, data)
	}
}
