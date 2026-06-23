package cluster

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
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

// TestVidDeterministicBlobNodeIDs_StableAcrossCompleters proves the recorded
// per-version blob NodeIDs are derived deterministically from the (bucket,key,
// vid) — NOT from a segment's random blobID — so two independent completers of
// the same upload record the SAME NodeIDs. That set is the per-version blob
// write target AND the hard-delete / tombstone-GC target, so divergence would
// orphan a loser's blob; vid-keying makes it converge. The det-vid is the same
// across completers (deriveMultipartVID), so the same vid → same NodeIDs.
//
// The fake groupSelector hashes the blobID argument (which vidDeterministicBlobNodeIDs
// passes as the vid) into the group index — mirroring the production
// hashSegmentPlacementKey logic — so the test exercises group-selection
// vid-determinism, not just PlacementForNodes permutation.
func TestVidDeterministicBlobNodeIDs_StableAcrossCompleters(t *testing.T) {
	groups := fourPGFixture()
	cfg := ECConfig{DataShards: 4, ParityShards: 2}

	// vidAwareSelector hashes (bucket, key, idx, blobID) to a group index,
	// matching the production SelectSegmentPlacementGroup hash. This ensures
	// group selection is keyed on the blobID/vid argument, not ignored.
	vidAwareSelector := func(bucket, key string, idx int, blobID string) (ShardGroupEntry, error) {
		if len(groups) == 0 {
			return ShardGroupEntry{}, fmt.Errorf("no groups")
		}
		i := hashSegmentPlacementKey(bucket, key, idx, blobID) % uint64(len(groups))
		return groups[i], nil
	}

	// Each "completer" is its own csb with INDEPENDENT random segment blobIDs.
	newCompleter := func() *clusterSegmentBackend {
		deps := newFakeBackendWithGroups(groups)
		csb := newCSBWithDeps(deps, []string{uuid.Must(uuid.NewV7()).String()})
		csb.ecConfigFn = func() ECConfig { return cfg }
		csb.groupSelectorFn = vidAwareSelector
		return csb
	}

	const bkt, key = "b", "k"
	// vid1 and vid2 are chosen to hash to DIFFERENT groups (idx 0 and 1
	// respectively with the fourPGFixture) so the distinct-group assertion is not
	// vacuous. Verified via hashSegmentPlacementKey("b","k",0,vid) % 4.
	const vid1 = "019ed400-0000-7000-8000-0000000000ab" // → group idx 0
	const vid2 = "019ed400-0001-7000-8000-0000000000ab" // → group idx 1

	a, err := newCompleter().vidDeterministicBlobNodeIDs(bkt, key, vid1, cfg)
	require.NoError(t, err)
	b, err := newCompleter().vidDeterministicBlobNodeIDs(bkt, key, vid1, cfg)
	require.NoError(t, err)
	require.Equal(t, a, b, "same (bucket,key,vid) → identical recorded NodeIDs across completers")
	require.Len(t, a, cfg.NumShards(), "recorded NodeIDs has K+M entries")

	// Assert group-selection vid-determinism: same vid always picks the same group.
	g1a, err := newCompleter().selectGroup(bkt, key, 0, vid1)
	require.NoError(t, err)
	g1b, err := newCompleter().selectGroup(bkt, key, 0, vid1)
	require.NoError(t, err)
	require.Equal(t, g1a.ID, g1b.ID, "same vid always selects the same group")

	// Assert that two distinct vids can land on different groups (group selection
	// actually hashes on vid, not a constant). With 4 groups and two chosen vids
	// that hash differently, they must not collide.
	g2, err := newCompleter().selectGroup(bkt, key, 0, vid2)
	require.NoError(t, err)
	require.NotEqual(t, g1a.ID, g2.ID, "distinct vids must select different groups (group selection keys on vid)")

	// A different vid is allowed to (and here does) produce different overall
	// NodeIDs — proving placement keys on vid end-to-end.
	other, err := newCompleter().vidDeterministicBlobNodeIDs(bkt, key, vid2, cfg)
	require.NoError(t, err)
	require.NotEqual(t, a, other, "a different vid keys to a different placement")
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
