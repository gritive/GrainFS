package cluster

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// TestScanQuorumMetaVersionsBucketAll_ReturnsEveryVersion proves the additive
// all-version enumerator returns EVERY decoded per-version blob (no max-per-key
// collapse), while the sibling ScanQuorumMetaVersionsBucket still folds to the
// per-key max. It also covers the prefix filter and the empty-bucket case.
func TestScanQuorumMetaVersionsBucketAll_ReturnsEveryVersion(t *testing.T) {
	b := newTestDistributedBackend(t)
	svc := b.shardSvc

	// One key with three versions v1<v2<v3 (distinct ModTime/MetaSeq).
	v1 := "019ed400-0000-7000-8000-000000000001"
	v2 := "019ed400-0000-7000-8000-000000000002"
	v3 := "019ed400-0000-7000-8000-000000000003"
	writeVerBlob(t, svc, "bkt", "obj", v1, PutObjectMetaCmd{ModTime: 100, MetaSeq: 1})
	writeVerBlob(t, svc, "bkt", "obj", v2, PutObjectMetaCmd{ModTime: 200, MetaSeq: 2})
	writeVerBlob(t, svc, "bkt", "obj", v3, PutObjectMetaCmd{ModTime: 300, MetaSeq: 3})

	// Max-per-key walker: exactly one entry, the max VersionID (v3).
	maxOnly, err := svc.ScanQuorumMetaVersionsBucket("bkt", "")
	require.NoError(t, err)
	require.Len(t, maxOnly, 1, "max-per-key walker collapses to one entry per key")
	require.Equal(t, v3, maxOnly[0].VersionID, "max-per-key walker keeps the max VersionID")

	// All-version walker: every blob (v1, v2, v3).
	all, err := svc.ScanQuorumMetaVersionsBucketAll("bkt", "")
	require.NoError(t, err)
	allVids := map[string]bool{}
	for _, c := range all {
		allVids[c.VersionID] = true
	}
	require.Equal(t, map[string]bool{v1: true, v2: true, v3: true}, allVids,
		"all-version walker returns every version blob")
	require.Len(t, all, 3, "no max-per-key collapse")

	// 2-key x 2-version case: All -> 4 entries, Bucket -> 2.
	b2 := newTestDistributedBackend(t)
	svc2 := b2.shardSvc
	writeVerBlob(t, svc2, "bkt", "k1", "019ed400-0000-7000-8000-00000000000a", PutObjectMetaCmd{ModTime: 1})
	writeVerBlob(t, svc2, "bkt", "k1", "019ed400-0000-7000-8000-00000000000b", PutObjectMetaCmd{ModTime: 2})
	writeVerBlob(t, svc2, "bkt", "k2", "019ed400-0000-7000-8000-00000000000c", PutObjectMetaCmd{ModTime: 3})
	writeVerBlob(t, svc2, "bkt", "k2", "019ed400-0000-7000-8000-00000000000d", PutObjectMetaCmd{ModTime: 4})
	all2, err := svc2.ScanQuorumMetaVersionsBucketAll("bkt", "")
	require.NoError(t, err)
	require.Len(t, all2, 4, "2 keys x 2 versions -> 4 entries (all)")
	max2, err := svc2.ScanQuorumMetaVersionsBucket("bkt", "")
	require.NoError(t, err)
	require.Len(t, max2, 2, "2 keys -> 2 entries (max-per-key)")

	// Empty bucket: All returns empty slice, nil error.
	b3 := newTestDistributedBackend(t)
	empty, err := b3.shardSvc.ScanQuorumMetaVersionsBucketAll("empty-bkt", "")
	require.NoError(t, err)
	require.Empty(t, empty, "empty bucket -> empty slice, nil error")

	// Prefix filter is on the decoded cmd.Key (mirrors the max-per-key prefix test).
	b4 := newTestDistributedBackend(t)
	svc4 := b4.shardSvc
	writeVerBlob(t, svc4, "bkt", "foo/1", "019ed400-0000-7000-8000-000000000010", PutObjectMetaCmd{})
	writeVerBlob(t, svc4, "bkt", "foo/1", "019ed400-0000-7000-8000-000000000011", PutObjectMetaCmd{})
	writeVerBlob(t, svc4, "bkt", "bar/1", "019ed400-0000-7000-8000-000000000012", PutObjectMetaCmd{})
	pref, err := svc4.ScanQuorumMetaVersionsBucketAll("bkt", "foo/")
	require.NoError(t, err)
	prefKeys := map[string]bool{}
	for _, c := range pref {
		prefKeys[c.Key] = true
	}
	require.Equal(t, map[string]bool{"foo/1": true}, prefKeys, "prefix filters on decoded cmd.Key")
	require.Len(t, pref, 2, "both foo/1 versions returned (no collapse), bar/1 excluded")
}

// --- S4c-a3 T3: all-versions peer RPC (round-trip + fail-closed) ---
func TestScanQuorumMetaVersionsAll_RPCRoundTrip(t *testing.T) {
	ctx := context.Background()
	keeper, clusterID := testDEKKeeper(t)

	trSelf := transport.MustNewHTTPTransport("test-cluster-psk")
	trPeer := transport.MustNewHTTPTransport("test-cluster-psk")
	require.NoError(t, trSelf.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, trPeer.Listen(ctx, "127.0.0.1:0"))
	defer trSelf.Close()
	defer trPeer.Close()

	svcSelf := NewShardService(t.TempDir(), trSelf, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	svcPeer := NewShardService(t.TempDir(), trPeer, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	trPeer.RegisterBufferedRoute(transport.RouteShardRPC, svcPeer.NativeRPCHandler())

	const bkt, key = "bkt", "a/b/c.txt"
	// Three versions of one key on the PEER's per-version store.
	for _, vid := range []string{
		"019ed400-0000-7000-8000-000000000001",
		"019ed400-0000-7000-8000-000000000002",
		"019ed400-0000-7000-8000-000000000003",
	} {
		blob, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{Bucket: bkt, Key: key, VersionID: vid, ETag: "e-" + vid})
		require.NoError(t, err)
		require.NoError(t, svcPeer.writeQuorumMetaVersionLocal(bkt, filepath.Join(key, vid), blob))
	}

	peerAddr := trPeer.LocalAddr()

	// All-version RPC: every version is enumerated.
	all, err := svcSelf.ScanQuorumMetaVersionsAll(ctx, peerAddr, bkt, "")
	require.NoError(t, err)
	gotAll := map[string]bool{}
	for _, c := range all {
		gotAll[c.VersionID] = true
	}
	require.Len(t, all, 3, "all-version RPC must return every per-version blob")
	require.Equal(t, map[string]bool{
		"019ed400-0000-7000-8000-000000000001": true,
		"019ed400-0000-7000-8000-000000000002": true,
		"019ed400-0000-7000-8000-000000000003": true,
	}, gotAll)

	// Max-per-key RPC on the SAME peer/data returns exactly 1 (proves the new RPC
	// is distinct and correctly wired to the all-version local scan).
	maxPerKey, err := svcSelf.ScanQuorumMetaVersions(ctx, peerAddr, bkt, "")
	require.NoError(t, err)
	require.Len(t, maxPerKey, 1, "max-per-key RPC collapses one key to its newest version")
	require.Equal(t, "019ed400-0000-7000-8000-000000000003", maxPerKey[0].VersionID)
}

// TestScanQuorumMetaVersionsAll_FailClosedOnUnsupported proves the send is
// fail-closed: a peer that returns an "Error" reply for the new msgType (an
// un-upgraded peer that doesn't know "ScanQuorumMetaVersionsAll") surfaces a
// NON-NIL error, NOT a silent empty result.
func TestScanQuorumMetaVersionsAll_FailClosedOnUnsupported(t *testing.T) {
	ctx := context.Background()
	keeper, clusterID := testDEKKeeper(t)

	trSelf := transport.MustNewHTTPTransport("test-cluster-psk")
	trPeer := transport.MustNewHTTPTransport("test-cluster-psk")
	require.NoError(t, trSelf.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, trPeer.Listen(ctx, "127.0.0.1:0"))
	defer trSelf.Close()
	defer trPeer.Close()

	svcSelf := NewShardService(t.TempDir(), trSelf, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	svcPeer := NewShardService(t.TempDir(), trPeer, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	// Simulate an UN-UPGRADED peer: its shard route replies "Error" for the new
	// msgType exactly as the pre-T3 default dispatch case would have
	// (errorResponse → marshalResponseDirect("Error", ...)).
	trPeer.RegisterBufferedRoute(transport.RouteShardRPC, func(payload []byte) ([]byte, error) {
		return svcPeer.errorResponse("unknown shard RPC: ScanQuorumMetaVersionsAll"), nil
	})

	out, err := svcSelf.ScanQuorumMetaVersionsAll(ctx, trPeer.LocalAddr(), "bkt", "")
	require.Error(t, err, "fail-closed: peer Error reply must surface a non-nil error")
	require.Nil(t, out, "fail-closed: must NOT degrade to an empty all-version result")
}
