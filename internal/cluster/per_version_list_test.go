package cluster

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// writeVerBlob writes a per-version quorum-meta blob for (bucket,key,vid) to the
// given ShardService's local versions subtree.
func writeVerBlob(t *testing.T, svc *ShardService, bucket, key, vid string, c PutObjectMetaCmd) {
	t.Helper()
	c.Bucket = bucket
	c.Key = key
	c.VersionID = vid
	blob, err := EncodeCommand(CmdPutObjectMeta, c)
	require.NoError(t, err)
	require.NoError(t, svc.writeQuorumMetaVersionLocal(bucket, filepath.Join(key, vid), blob))
}

// TestScanQuorumMetaVersionsBucket_GroupsByDecodedKey proves the walker returns
// ONE max-vid entry per distinct decoded cmd.Key. The critical case: nested keys
// "a/b" and "a/b/c.txt" — the dir ".quorum_meta_versions/{bkt}/a/b/" is BOTH a
// key-leaf dir (holds a/b's vid files) AND an intermediate dir (holds c.txt/), so
// grouping must be by decoded cmd.Key, not dir structure.
func TestScanQuorumMetaVersionsBucket_GroupsByDecodedKey(t *testing.T) {
	b := newTestDistributedBackend(t)
	svc := b.shardSvc

	// key "a/b": v1 then v2 (newer) — max-vid must win.
	writeVerBlob(t, svc, "bkt", "a/b", "019ed400-0000-7000-8000-000000000001", PutObjectMetaCmd{ETag: "ab-v1"})
	writeVerBlob(t, svc, "bkt", "a/b", "019ed400-0000-7000-8000-000000000002", PutObjectMetaCmd{ETag: "ab-v2"})
	// key "a/b/c.txt": lives UNDER a/b's leaf dir — must be its own key.
	writeVerBlob(t, svc, "bkt", "a/b/c.txt", "019ed400-0000-7000-8000-000000000003", PutObjectMetaCmd{ETag: "abc-v1"})
	// key "x": local-max is a delete marker — walker MUST still return it (merge excludes later).
	writeVerBlob(t, svc, "bkt", "x", "019ed400-0000-7000-8000-000000000004", PutObjectMetaCmd{ETag: "x-v1"})
	writeVerBlob(t, svc, "bkt", "x", "019ed400-0000-7000-8000-000000000005", PutObjectMetaCmd{IsDeleteMarker: true})

	got, err := svc.ScanQuorumMetaVersionsBucket("bkt", "")
	require.NoError(t, err)

	byKey := map[string]PutObjectMetaCmd{}
	for _, c := range got {
		_, dup := byKey[c.Key]
		require.False(t, dup, "key %s returned more than once", c.Key)
		byKey[c.Key] = c
	}
	require.Len(t, byKey, 3, "three distinct keys: a/b, a/b/c.txt, x")
	require.Equal(t, "ab-v2", byKey["a/b"].ETag, "a/b max-vid = v2")
	require.Equal(t, "abc-v1", byKey["a/b/c.txt"].ETag, "a/b/c.txt is its own key (not folded into a/b)")
	require.True(t, byKey["x"].IsDeleteMarker, "x local-max is the marker (walker includes markers)")
}

// TestScanQuorumMetaVersionsBucket_PrefixFilter proves the prefix filter is on
// the decoded cmd.Key.
func TestScanQuorumMetaVersionsBucket_PrefixFilter(t *testing.T) {
	b := newTestDistributedBackend(t)
	svc := b.shardSvc
	writeVerBlob(t, svc, "bkt", "foo/1", "019ed400-0000-7000-8000-000000000001", PutObjectMetaCmd{})
	writeVerBlob(t, svc, "bkt", "foo/2", "019ed400-0000-7000-8000-000000000002", PutObjectMetaCmd{})
	writeVerBlob(t, svc, "bkt", "bar/1", "019ed400-0000-7000-8000-000000000003", PutObjectMetaCmd{})

	got, err := svc.ScanQuorumMetaVersionsBucket("bkt", "foo/")
	require.NoError(t, err)
	keys := map[string]bool{}
	for _, c := range got {
		keys[c.Key] = true
	}
	require.Equal(t, map[string]bool{"foo/1": true, "foo/2": true}, keys)
}

// TestScanQuorumMetaVersions_RPC proves the per-version bucket walker RPC
// enumerates a remote node's per-version blobs grouped by key.
func TestScanQuorumMetaVersions_RPC(t *testing.T) {
	ctx := context.Background()
	keeper, clusterID := testDEKKeeper(t)

	trA := transport.MustNewHTTPTransport("test-cluster-psk")
	trB := transport.MustNewHTTPTransport("test-cluster-psk")
	require.NoError(t, trA.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, trB.Listen(ctx, "127.0.0.1:0"))
	defer trA.Close()
	defer trB.Close()

	svcA := NewShardService(t.TempDir(), trA, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	svcB := NewShardService(t.TempDir(), trB, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	trB.RegisterBufferedRoute(transport.RouteShardRPC, svcB.NativeRPCHandler())

	writeVerBlob(t, svcB, "bkt", "k1", "019ed400-0000-7000-8000-000000000001", PutObjectMetaCmd{ETag: "k1-v1"})
	writeVerBlob(t, svcB, "bkt", "k1", "019ed400-0000-7000-8000-000000000002", PutObjectMetaCmd{ETag: "k1-v2"})
	writeVerBlob(t, svcB, "bkt", "k2", "019ed400-0000-7000-8000-000000000003", PutObjectMetaCmd{ETag: "k2-v1"})

	got, err := svcA.ScanQuorumMetaVersions(ctx, trB.LocalAddr(), "bkt", "")
	require.NoError(t, err)
	byKey := map[string]string{}
	for _, c := range got {
		byKey[c.Key] = c.ETag
	}
	require.Equal(t, map[string]string{"k1": "k1-v2", "k2": "k2-v1"}, byKey, "max-vid per key over the wire")
}
