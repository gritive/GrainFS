package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCollectECRewrapTargets_NonVersionedLatestBlob proves the DEK-rewrap
// enumeration covers a NON-VERSIONED object that exists only as a latest-only blob
// (no FSM obj: record, no per-version blob). Without this, KEK rotation would
// silently skip every non-versioned object's shards — leaving them encrypted under
// a retired KEK generation (undecryptable once that gen is pruned). A hard-delete
// tombstone on the latest-only key is excluded (no live shards).
func TestCollectECRewrapTargets_NonVersionedLatestBlob(t *testing.T) {
	backend, _ := setupECRewrapBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "nv"))
	// No SetBucketVersioning → non-versioned (latest-only authority).
	self := backend.selfAddr

	seedLatestBlob(t, backend, "nv", "k", PutObjectMetaCmd{
		VersionID: "v1", ETag: "e", ECData: 1, ECParity: 0, NodeIDs: []string{self},
	})
	seedLatestBlob(t, backend, "nv", "gone", PutObjectMetaCmd{
		VersionID: "vx", ETag: "e", ECData: 1, ECParity: 0, NodeIDs: []string{self}, IsHardDeleted: true,
	})

	targets, err := backend.CollectECRewrapTargets()
	require.NoError(t, err)
	var nvKeys []string
	for _, tgt := range targets {
		if tgt.Bucket == "nv" {
			nvKeys = append(nvKeys, tgt.ShardKey)
		}
	}
	require.Contains(t, nvKeys, ecObjectShardKey("k", "v1"),
		"non-versioned latest-only blob must be enumerated for DEK rewrap")
	require.NotContains(t, nvKeys, ecObjectShardKey("gone", "vx"),
		"hard-delete tombstone must not be enumerated for rewrap")
}
