package cluster

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// TestReadQuorumMetaVersions_SameVIDHigherModTimeWins proves the same-(key,vid)
// dedup at the readers is deterministic by LWW (quorumMetaCmdWins), not the old
// "MetaSeq >= keeps last-iterated" tiebreak. Two replicas of ONE (key,vid) carry
// the SAME MetaSeq (0) but different ModTime: the higher ModTime must win
// regardless of fan-out iteration order. RED with the old MetaSeq>= sub-branch:
// the peer (lower ModTime, iterated last) overwrote the local higher-ModTime
// winner because 0 >= 0 is true.
func TestReadQuorumMetaVersions_SameVIDHigherModTimeWins(t *testing.T) {
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
	trA.RegisterBufferedRoute(transport.RouteShardRPC, svcA.NativeRPCHandler())
	trB.RegisterBufferedRoute(transport.RouteShardRPC, svcB.NativeRPCHandler())

	const bkt, key, vid = "bkt", "k", "019ed400-0000-7000-8000-000000000001"
	write := func(svc *ShardService, modTime int64, etag string) {
		t.Helper()
		blob, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
			Bucket: bkt, Key: key, VersionID: vid, ETag: etag,
			ModTime: modTime, MetaSeq: 0, NodeIDs: []string{"n"}, ECData: 1,
		})
		require.NoError(t, err)
		require.NoError(t, svc.writeQuorumMetaVersionLocal(bkt, filepath.Join(key, vid), blob))
	}
	// Local (self, iterated FIRST) carries the HIGHER ModTime winner; the peer
	// (iterated LAST) carries the stale lower-ModTime replica. The old >= tiebreak
	// kept the last-iterated (peer) loser; LWW keeps the higher-ModTime local one.
	write(svcA, 200, "winner")
	write(svcB, 100, "loser")

	backendA := &DistributedBackend{
		selfAddr: trA.LocalAddr(),
		shardSvc: svcA,
		shardGroup: &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
			"g1": {ID: "g1", PeerIDs: []string{trA.LocalAddr()}},
			"g2": {ID: "g2", PeerIDs: []string{trB.LocalAddr()}},
		}},
	}

	cmds, err := backendA.readQuorumMetaVersions(bkt, key)
	require.NoError(t, err)
	require.Len(t, cmds, 1, "same (key,vid) replicas dedup to one")
	require.Equal(t, int64(200), cmds[0].ModTime, "higher ModTime wins (deterministic LWW, not last-iterated)")
	require.Equal(t, "winner", cmds[0].ETag)
}

// TestQuorumMetaCmdWins_RelocationInvariant pins the relocation invariant the
// readers rely on: on an equal ModTime AND VersionID, the higher MetaSeq wins, so
// a placement re-write (relocation) that preserves ModTime+VID still wins
// deterministically over the old replica it supersedes.
func TestQuorumMetaCmdWins_RelocationInvariant(t *testing.T) {
	old := PutObjectMetaCmd{ModTime: 100, VersionID: "v1", MetaSeq: 0}
	relocated := PutObjectMetaCmd{ModTime: 100, VersionID: "v1", MetaSeq: 1}
	require.True(t, quorumMetaCmdWins(relocated, old), "same ModTime+VID, higher MetaSeq (relocation) wins")
	require.False(t, quorumMetaCmdWins(old, relocated), "the superseded replica must not win")
}
