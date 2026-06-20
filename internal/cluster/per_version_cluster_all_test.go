package cluster

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// TestScanQuorumMetaVersionsClusterAll_ReturnsAllVersions proves the cluster-wide
// all-version fan-in returns EVERY seeded per-version blob (NOT collapsed to the
// per-key max), across multiple keys and versions, including a delete-marker blob.
// Single-node harness: shardGroup == nil → the fan-in is the local STRICT scan.
func TestScanQuorumMetaVersionsClusterAll_ReturnsAllVersions(t *testing.T) {
	b := newTestDistributedBackend(t)
	svc := b.shardSvc

	// key "k1": v1 (live) + v2 (live)
	k1v1 := "019ed400-0000-7000-8000-000000000001"
	k1v2 := "019ed400-0000-7000-8000-000000000002"
	writeVerBlob(t, svc, "bkt", "k1", k1v1, PutObjectMetaCmd{ETag: "k1-v1", ModTime: 100, MetaSeq: 1})
	writeVerBlob(t, svc, "bkt", "k1", k1v2, PutObjectMetaCmd{ETag: "k1-v2", ModTime: 200, MetaSeq: 2})

	// key "k2": v1 (live) + v2 (DELETE MARKER)
	k2v1 := "019ed400-0000-7000-8000-00000000000a"
	k2v2 := "019ed400-0000-7000-8000-00000000000b"
	writeVerBlob(t, svc, "bkt", "k2", k2v1, PutObjectMetaCmd{ETag: "k2-v1", ModTime: 300, MetaSeq: 3})
	writeVerBlob(t, svc, "bkt", "k2", k2v2, PutObjectMetaCmd{ETag: deleteMarkerETag, IsDeleteMarker: true, ModTime: 400, MetaSeq: 4})

	out, err := b.scanQuorumMetaVersionsClusterAll("bkt", "")
	require.NoError(t, err)

	byVID := map[string]PutObjectMetaCmd{}
	for _, c := range out {
		byVID[c.VersionID] = c
	}
	require.Len(t, out, 4, "all-version fan-in must return every version (no max-per-key collapse)")
	require.Equal(t, map[string]bool{k1v1: true, k1v2: true, k2v1: true, k2v2: true},
		map[string]bool{k1v1: byVID[k1v1].VersionID == k1v1, k1v2: byVID[k1v2].VersionID == k1v2,
			k2v1: byVID[k2v1].VersionID == k2v1, k2v2: byVID[k2v2].VersionID == k2v2})
	require.True(t, byVID[k2v2].IsDeleteMarker, "delete-marker blob is enumerated, not collapsed away")
}

// TestScanQuorumMetaVersionsClusterAll_PrefixFilter proves the prefix filter is
// applied (on the decoded cmd.Key) by the underlying strict local scan.
func TestScanQuorumMetaVersionsClusterAll_PrefixFilter(t *testing.T) {
	b := newTestDistributedBackend(t)
	svc := b.shardSvc

	writeVerBlob(t, svc, "bkt", "foo/1", "019ed400-0000-7000-8000-000000000010", PutObjectMetaCmd{})
	writeVerBlob(t, svc, "bkt", "foo/1", "019ed400-0000-7000-8000-000000000011", PutObjectMetaCmd{})
	writeVerBlob(t, svc, "bkt", "bar/1", "019ed400-0000-7000-8000-000000000012", PutObjectMetaCmd{})

	out, err := b.scanQuorumMetaVersionsClusterAll("bkt", "foo/")
	require.NoError(t, err)
	keys := map[string]bool{}
	for _, c := range out {
		keys[c.Key] = true
	}
	require.Equal(t, map[string]bool{"foo/1": true}, keys, "prefix filters on decoded cmd.Key")
	require.Len(t, out, 2, "both foo/1 versions returned (no collapse), bar/1 excluded")
}

// TestScanQuorumMetaVersionsClusterAll_AbsentBucketEmpty proves an absent bucket
// dir is an empty SUCCESS (not an error) — the absent-bucket-empty invariant the
// strict local scan preserves must survive the cluster-wide wrapper.
func TestScanQuorumMetaVersionsClusterAll_AbsentBucketEmpty(t *testing.T) {
	b := newTestDistributedBackend(t)
	out, err := b.scanQuorumMetaVersionsClusterAll("never-created", "")
	require.NoError(t, err, "absent bucket must be an empty success, not an error")
	require.Empty(t, out)
}

// TestScanQuorumMetaVersionsClusterAll_FailsClosedOnCorruptLocalBlob proves the
// FAIL-CLOSED property: an undecodable LOCAL per-version blob makes the cluster-wide
// fan-in return an error instead of a silently-truncated set. Under sole authority
// a dropped version would be silent data loss.
func TestScanQuorumMetaVersionsClusterAll_FailsClosedOnCorruptLocalBlob(t *testing.T) {
	b := newTestDistributedBackend(t)
	svc := b.shardSvc

	writeVerBlob(t, svc, "bkt", "k", "019ed400-0000-7000-8000-000000000001", PutObjectMetaCmd{ETag: "ok"})
	writeRawVersionBlobForTest(t, svc, "bkt", "k", "019ed400-0000-7000-8000-000000000002", []byte("\x00garbage"))

	_, err := b.scanQuorumMetaVersionsClusterAll("bkt", "")
	require.Error(t, err, "fail-closed: an undecodable local blob must surface an error")
}

// TestHandleScanQuorumMetaVersionsAll_PropagatesStrictError proves the peer RPC
// handler is now FAIL-CLOSED end-to-end: an undecodable per-version blob on the
// PEER makes the ScanQuorumMetaVersionsAll RPC return a non-nil error to the
// caller (a strict-scan "Error" reply), instead of a silently-truncated list.
func TestHandleScanQuorumMetaVersionsAll_PropagatesStrictError(t *testing.T) {
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

	const bkt = "bkt"
	// One good blob + one undecodable blob on the PEER.
	writeVerBlob(t, svcPeer, bkt, "k", "019ed400-0000-7000-8000-000000000001", PutObjectMetaCmd{ETag: "ok"})
	writeRawVersionBlobForTest(t, svcPeer, bkt, "k", "019ed400-0000-7000-8000-000000000002", []byte("\x00garbage"))

	out, err := svcSelf.ScanQuorumMetaVersionsAll(ctx, trPeer.LocalAddr(), bkt, "")
	require.Error(t, err, "fail-closed: a peer-side undecodable blob must surface a non-nil error")
	require.Nil(t, out, "fail-closed: must NOT degrade to a silently-truncated list")
}

// TestScanQuorumMetaVersionsAll_FailsClosedOnClientDecodeError proves the
// CLIENT side is also fail-closed end-to-end: a peer that returns an OK response
// whose packed blob list contains an undecodable entry (e.g. in-transit
// corruption, or a decode-version skew) must surface a non-nil error, NOT a
// silently-truncated authoritative version list. (codex code-gate [P1].)
func TestScanQuorumMetaVersionsAll_FailsClosedOnClientDecodeError(t *testing.T) {
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

	good, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "019ed400-0000-7000-8000-000000000001", ETag: "ok"})
	require.NoError(t, err)
	// Peer returns a well-formed OK response carrying one valid blob + one corrupt
	// blob — exactly what a strict handler would never pack, but transit corruption
	// can produce. The client must reject the whole response.
	trPeer.RegisterBufferedRoute(transport.RouteShardRPC, func(_ []byte) ([]byte, error) {
		return svcPeer.okResponse(packBlobList([][]byte{good, []byte("\x00corrupt-blob")})), nil
	})

	out, err := svcSelf.ScanQuorumMetaVersionsAll(ctx, trPeer.LocalAddr(), "bkt", "")
	require.Error(t, err, "fail-closed: a client-side undecodable blob in an OK response must error")
	require.Nil(t, out, "fail-closed: must NOT return a partial list")
}
