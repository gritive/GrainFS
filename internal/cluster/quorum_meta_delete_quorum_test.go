package cluster

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/transport"
)

// TestDeleteQuorumMetaQuorum_RemovesPeerReplica pins the fix for the cluster
// append-after-plain-PUT bug: the quorum-meta is K-of-N replicated across the
// object's placement nodes, so the post-append cleanup MUST delete the replica
// on EVERY node. A local-only delete (the pre-fix behavior) leaves a stale
// replica on the peer, and headObjectMeta reads quorum-meta first with peer
// fan-out — so the peer's stale copy shadows the BadgerDB append, making the
// append invisible on a multi-node cluster (passes on single-node because the
// only replica is local).
func TestDeleteQuorumMetaQuorum_RemovesPeerReplica(t *testing.T) {
	ctx := context.Background()
	keeper, clusterID := testDEKKeeper(t)

	// Two real TCP transport nodes. node-self runs the delete; node-peer holds a
	// replica that must be removed via the DeleteQuorumMeta RPC.
	trSelf := transport.MustNewHTTPTransport("test-cluster-psk")
	trPeer := transport.MustNewHTTPTransport("test-cluster-psk")
	require.NoError(t, trSelf.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, trPeer.Listen(ctx, "127.0.0.1:0"))
	defer trSelf.Close()
	defer trPeer.Close()

	dirSelf := t.TempDir()
	dirPeer := t.TempDir()
	svcSelf := NewShardService(dirSelf, trSelf,
		WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	svcPeer := NewShardService(dirPeer, trPeer,
		WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	// node-peer serves incoming shard RPCs (including DeleteQuorumMeta).
	trPeer.RegisterBufferedRoute(transport.RouteShardRPC, svcPeer.NativeRPCHandler())

	// Replicate the quorum-meta blob to BOTH nodes (K-of-N write landed on both).
	blob := func() []byte {
		cmd := PutObjectMetaCmd{
			Bucket:  "bkt",
			Key:     "obj",
			ECData:  1,
			Size:    5,
			ETag:    "abc123",
			NodeIDs: []string{trSelf.LocalAddr(), trPeer.LocalAddr()},
		}
		b, err := EncodeCommand(CmdPutObjectMeta, cmd)
		require.NoError(t, err)
		return b
	}()
	require.NoError(t, svcSelf.writeQuorumMetaLocal("bkt", "obj", blob))
	require.NoError(t, svcPeer.writeQuorumMetaLocal("bkt", "obj", blob))

	selfPath := filepath.Join(dirSelf, "shards", quorumMetaSubDir, "bkt", "obj")
	peerPath := filepath.Join(dirPeer, "shards", quorumMetaSubDir, "bkt", "obj")
	require.FileExists(t, selfPath, "precondition: self replica present")
	require.FileExists(t, peerPath, "precondition: peer replica present")

	backendSelf := &DistributedBackend{
		selfAddr: trSelf.LocalAddr(),
		shardSvc: svcSelf,
	}

	// Fan-out delete across both placement nodes.
	backendSelf.deleteQuorumMetaQuorum(ctx, "bkt", "obj", []string{trSelf.LocalAddr(), trPeer.LocalAddr()})

	require.NoFileExists(t, selfPath, "self replica must be deleted locally")
	require.NoFileExists(t, peerPath, "peer replica must be deleted via DeleteQuorumMeta RPC (the bug: local-only delete leaves this)")
}

// TestDeleteQuorumMetaQuorum_EmptyNodesFallsBackLocal verifies the BadgerDB-only
// path (object never had a quorum-meta replica set) still removes any local file.
func TestDeleteQuorumMetaQuorum_EmptyNodesFallsBackLocal(t *testing.T) {
	ctx := context.Background()
	keeper, clusterID := testDEKKeeper(t)
	tr := transport.MustNewHTTPTransport("test-cluster-psk")
	require.NoError(t, tr.Listen(ctx, "127.0.0.1:0"))
	defer tr.Close()
	dir := t.TempDir()
	svc := NewShardService(dir, tr,
		WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	require.NoError(t, svc.writeQuorumMetaLocal("bkt", "obj", []byte("x")))
	p := filepath.Join(dir, "shards", quorumMetaSubDir, "bkt", "obj")
	require.FileExists(t, p)

	b := &DistributedBackend{selfAddr: tr.LocalAddr(), shardSvc: svc}
	b.deleteQuorumMetaQuorum(ctx, "bkt", "obj", nil)

	_, err := os.Stat(p)
	require.True(t, os.IsNotExist(err), "empty nodeIDs must still delete the local replica")
}
