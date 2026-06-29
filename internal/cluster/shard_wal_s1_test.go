package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// newS1ShardSvc builds a DistributedBackend + ShardService with the given EC
// config. It returns the shard dir + the DEK keeper/clusterID for callers that
// need them. The shard WAL was removed in S4; durability is write-time
// fsync (small / no-redundancy) or EC (large redundant). extraOpts lets a
// caller wire WithNoRedundancy.
func newS1ShardSvc(t *testing.T, ec ECConfig, nodes []string, extraOpts ...ShardServiceOption) (*DistributedBackend, string, *encrypt.DEKKeeper, []byte) {
	t.Helper()
	shardDir := t.TempDir()
	keeper, clusterID := testDEKKeeper(t)
	opts := append([]ShardServiceOption{WithShardDEKKeeper(keeper, clusterID)}, extraOpts...)
	svc := NewShardService(shardDir, nil, opts...)
	backend := NewSingletonBackendForTest(t)
	backend.shardSvc = svc
	backend.selfAddr = "self"
	backend.allNodes = nodes
	backend.SetECConfig(ec)
	require.NoError(t, backend.CreateBucket(context.Background(), "b"))
	return backend, shardDir, keeper, clusterID
}
