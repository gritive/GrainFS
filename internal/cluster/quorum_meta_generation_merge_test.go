package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/transport"
)

// twoGenerationBackend wires two shard services on separate transports into a
// single DistributedBackend whose ShardGroups() spans both groups — modeling
// generation-0 (group g1) and generation-1 (group g2). selfAddr decides which
// service is "local" (the routed leader). Mirrors the S7-5
// TestScatterGatherList_SpansAllShardGroups harness.
func twoGenerationBackend(t *testing.T, selfIsB bool) (*DistributedBackend, *ShardService, *ShardService) {
	t.Helper()
	ctx := context.Background()
	keeper, clusterID := testDEKKeeper(t)

	trA := transport.MustNewTCPTransport("test-cluster-psk")
	trB := transport.MustNewTCPTransport("test-cluster-psk")
	require.NoError(t, trA.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, trB.Listen(ctx, "127.0.0.1:0"))
	t.Cleanup(func() { trA.Close() })
	t.Cleanup(func() { trB.Close() })
	require.NoError(t, trA.Connect(ctx, trB.LocalAddr()))
	require.NoError(t, trB.Connect(ctx, trA.LocalAddr()))

	svcA := NewShardService(t.TempDir(), trA, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	svcB := NewShardService(t.TempDir(), trB, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	trA.SetStreamHandler(svcA.HandleRPC())
	trB.SetStreamHandler(svcB.HandleRPC())

	selfAddr := trA.LocalAddr()
	selfSvc := svcA
	if selfIsB {
		selfAddr = trB.LocalAddr()
		selfSvc = svcB
	}
	b := &DistributedBackend{
		selfAddr: selfAddr,
		shardSvc: selfSvc,
		shardGroup: &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
			"g1": {ID: "g1", PeerIDs: []string{trA.LocalAddr()}},
			"g2": {ID: "g2", PeerIDs: []string{trB.LocalAddr()}},
		}},
	}
	return b, svcA, svcB
}

func writeQMeta(t *testing.T, svc *ShardService, cmd PutObjectMetaCmd) {
	t.Helper()
	blob, err := EncodeCommand(CmdPutObjectMeta, cmd)
	require.NoError(t, err)
	require.NoError(t, svc.writeQuorumMetaLocal(cmd.Bucket, cmd.Key, blob))
}

// TestQuorumMetaMerge_CrossGenerationLWW proves the S7-6 fence: a routed
// (generation-1) leader holding a stale local copy must, when multi-generation,
// merge in the cross-generation peer fan-out and return the higher-ModTime copy
// that an add-window write landed in generation-0. At a single generation
// (multiGeneration false) it stays on the local-first fast path — byte-identical
// to legacy — returning its own stale copy.
//
// RED-on-revert: neuter readQuorumMetaWinningRaw's multiGeneration branch to
// always take the local-first path → the multiGeneration=true case returns the
// stale ModTime=100 copy and every assertion below fails.
func TestQuorumMetaMerge_CrossGenerationLWW(t *testing.T) {
	// self = generation-1 leader (svcB). svcA (gen-0) holds the fresher copy.
	b, svcA, svcB := twoGenerationBackend(t, true)
	writeQMeta(t, svcA, PutObjectMetaCmd{
		Bucket: "bkt", Key: "k", ETag: "fresh", VersionID: "v-gen0",
		ModTime: 200, ECData: 1, NodeIDs: []string{"nA"},
	})
	writeQMeta(t, svcB, PutObjectMetaCmd{
		Bucket: "bkt", Key: "k", ETag: "stale", VersionID: "v-gen1",
		ModTime: 100, ECData: 1, NodeIDs: []string{"nB"},
	})

	// Single generation: local-first fast path returns svcB's stale local copy.
	b.SetMultiGeneration(false)
	obj, _, err := b.readQuorumMeta("bkt", "k")
	require.NoError(t, err)
	require.Equal(t, int64(100), obj.LastModified, "single-gen: local-first fast path")
	require.Equal(t, "stale", obj.ETag)
	cmd, err := b.readQuorumMetaCmd("bkt", "k")
	require.NoError(t, err)
	require.Equal(t, int64(100), cmd.ModTime, "single-gen readQuorumMetaCmd: local-first")

	// Multi generation: merge picks the fresher gen-0 copy across generations.
	b.SetMultiGeneration(true)
	obj, pm, err := b.readQuorumMeta("bkt", "k")
	require.NoError(t, err)
	require.Equal(t, int64(200), obj.LastModified, "multi-gen: cross-generation LWW winner")
	require.Equal(t, "fresh", obj.ETag)
	require.Equal(t, []string{"nA"}, pm.NodeIDs, "winner's placement NodeIDs drive the data fetch")
	cmd, err = b.readQuorumMetaCmd("bkt", "k")
	require.NoError(t, err)
	require.Equal(t, int64(200), cmd.ModTime, "multi-gen readQuorumMetaCmd: cross-generation LWW")
	require.Equal(t, "v-gen0", cmd.VersionID)
}

// TestQuorumMetaMerge_CrossGenerationTombstone proves tombstones participate in
// the cross-generation LWW: a delete marker written to a newer generation with a
// higher ModTime wins over a live lower-ModTime copy in an older generation, so
// headObjectMeta later folds it to ErrObjectNotFound. Without the merge the
// routed leader's local live copy would shadow the delete.
func TestQuorumMetaMerge_CrossGenerationTombstone(t *testing.T) {
	// self = gen-0 leader (svcA) holding the live copy; svcB holds the tombstone.
	b, svcA, svcB := twoGenerationBackend(t, false)
	writeQMeta(t, svcA, PutObjectMetaCmd{
		Bucket: "bkt", Key: "k", ETag: "live", VersionID: "v-live",
		ModTime: 100, ECData: 1, NodeIDs: []string{"nA"},
	})
	writeQMeta(t, svcB, PutObjectMetaCmd{
		Bucket: "bkt", Key: "k", VersionID: "v-del",
		ModTime: 200, IsDeleteMarker: true, ECData: 1, NodeIDs: []string{"nA"},
	})

	b.SetMultiGeneration(true)
	obj, _, err := b.readQuorumMeta("bkt", "k")
	require.NoError(t, err)
	require.True(t, obj.IsDeleteMarker, "higher-ModTime tombstone wins the cross-generation LWW")
	require.Equal(t, int64(200), obj.LastModified)
}

// TestQuorumMetaBlobWins_VersionIDTiebreak proves the deterministic tiebreak
// (advisor D): on equal ModTime (second granularity) the lexicographically
// greater VersionID wins, giving point-GET and scatter-gather LIST a single
// shared ordering for same-second cross-generation ties.
func TestQuorumMetaBlobWins_VersionIDTiebreak(t *testing.T) {
	require.True(t, quorumMetaBlobWins(200, "v1", 100, "v9"), "higher ModTime wins regardless of VersionID")
	require.False(t, quorumMetaBlobWins(100, "v9", 200, "v1"), "lower ModTime loses regardless of VersionID")
	require.True(t, quorumMetaBlobWins(100, "v9", 100, "v1"), "equal ModTime: higher VersionID wins")
	require.False(t, quorumMetaBlobWins(100, "v1", 100, "v9"), "equal ModTime: lower VersionID loses")
	require.False(t, quorumMetaBlobWins(100, "v1", 100, "v1"), "identical: not strictly greater")
}

// TestQuorumMetaMerge_SameModTimeTiebreak proves the tiebreak resolves a
// same-second cross-generation tie deterministically through the merge: with
// equal ModTime the higher VersionID wins, the same outcome scatterGatherList
// would pick.
func TestQuorumMetaMerge_SameModTimeTiebreak(t *testing.T) {
	// self = svcB (lower VersionID); svcA holds the equal-ModTime higher VersionID.
	b, svcA, svcB := twoGenerationBackend(t, true)
	writeQMeta(t, svcA, PutObjectMetaCmd{
		Bucket: "bkt", Key: "k", ETag: "win", VersionID: "v-zzz",
		ModTime: 100, ECData: 1, NodeIDs: []string{"nA"},
	})
	writeQMeta(t, svcB, PutObjectMetaCmd{
		Bucket: "bkt", Key: "k", ETag: "lose", VersionID: "v-aaa",
		ModTime: 100, ECData: 1, NodeIDs: []string{"nB"},
	})

	b.SetMultiGeneration(true)
	obj, _, err := b.readQuorumMeta("bkt", "k")
	require.NoError(t, err)
	require.Equal(t, "win", obj.ETag, "equal ModTime: higher VersionID wins deterministically")
	require.Equal(t, "v-zzz", obj.VersionID)
}

// TestScatterGatherList_SameModTimeTiebreak proves the LIST path uses the same
// deterministic tiebreak as point-GET (advisor D): when the same key appears on
// two groups with equal ModTime, scatterGatherList keeps the higher VersionID —
// the same winner readQuorumMeta's merge picks — so point-GET and LIST agree.
// This tiebreak is generation-independent (it only changes outcomes that were
// already nondeterministic at second granularity).
//
// RED-on-revert: restore the strict `e.ModTime > cur.ModTime` compare → the
// gather-order (first responder) wins and the higher-VersionID assertion flakes/fails.
func TestScatterGatherList_SameModTimeTiebreak(t *testing.T) {
	b, svcA, svcB := twoGenerationBackend(t, false)
	writeQMeta(t, svcA, PutObjectMetaCmd{
		Bucket: "bkt", Key: "k", ETag: "lose", VersionID: "v-aaa",
		ModTime: 100, ECData: 1, NodeIDs: []string{"nA"},
	})
	writeQMeta(t, svcB, PutObjectMetaCmd{
		Bucket: "bkt", Key: "k", ETag: "win", VersionID: "v-zzz",
		ModTime: 100, ECData: 1, NodeIDs: []string{"nB"},
	})

	entries, err := b.scatterGatherList(context.Background(), "bkt", "")
	require.NoError(t, err)
	require.Len(t, entries, 1, "same key on two groups collapses to one LWW winner")
	require.Equal(t, "v-zzz", entries[0].VersionID, "equal ModTime: higher VersionID wins deterministically")
	require.Equal(t, "win", entries[0].ETag)
}

// TestAddPlacementGeneration_FiresCoordinatorRebuild proves S7-6 touch point 1b:
// applying an AddPlacementGeneration command on a live MetaFSM fires the
// coordinator's post-commit hook, which re-runs rebuild() and arms the
// multi-generation flag on the data-group backend WITHOUT any external rebuild
// call. Without the registered hook the applied generation would stay inert.
//
// RED-on-revert: drop registerTopologyRebuildHook() (or its cmdType guard) →
// the backend's multiGeneration flag never flips and the assertion fails.
func TestAddPlacementGeneration_FiresCoordinatorRebuild(t *testing.T) {
	base := &fakeBackend{}
	gb := newTestFollowerGroupBackend(t, "g1", "self")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("g1", []string{"self"}, gb))
	router := NewRouter(mgr)
	fsm := NewMetaFSM()

	NewClusterCoordinator(base, mgr, router, fsm, "self")
	require.False(t, gb.multiGeneration.Load(), "single generation at construction")

	// First generation → still single (count 1).
	require.NoError(t, fsm.applyCmd(makeAddPlacementGenerationCmd(t, []string{"g1"})))
	require.False(t, gb.multiGeneration.Load(), "one generation does not arm the merge")

	// Second generation → multi (count 2). The hook flips the backend flag with
	// no external rebuild call.
	require.NoError(t, fsm.applyCmd(makeAddPlacementGenerationCmd(t, []string{"g1", "g2"})))
	require.True(t, gb.multiGeneration.Load(), "AddPlacementGeneration apply arms the cross-generation merge via the post-commit hook")
}

// TestAddTopologyGeneration_CapturesGenZeroBeforeExpanded proves S7-6 touch
// point 2 (must-solve ②): the first growth captures gen-0 (the base group set)
// BEFORE recording the expanded generation, so existing objects placed by the
// original modulo stay readable; a later growth appends only the expanded set
// without recapturing the base. The ordering is what makes every intermediate
// crash state safe.
//
// RED-on-revert: drop the registry-empty gen-0 capture in AddTopologyGeneration
// → the first growth records only the expanded set (len 1, gens[0]=expanded) and
// the base-captured assertion fails.
func TestAddTopologyGeneration_CapturesGenZeroBeforeExpanded(t *testing.T) {
	node := newFakeDEKNode(true)
	m, _ := newTestMetaRaftForDEK(t, node)
	require.Empty(t, m.fsm.PlacementGenerations())

	// First growth: empty registry → capture gen-0 = base, then expanded.
	require.NoError(t, m.AddTopologyGeneration(context.Background(),
		[]string{"g1", "g2"}, []string{"g1", "g2", "g3", "g4"}))
	gens := m.fsm.PlacementGenerations()
	require.Len(t, gens, 2)
	require.Equal(t, []string{"g1", "g2"}, gens[0].groupIDs, "gen-0 = base group set captured first")
	require.Equal(t, []string{"g1", "g2", "g3", "g4"}, gens[1].groupIDs, "gen-1 = expanded group set")

	// Second growth: non-empty registry → append only the expanded set, no
	// base recapture (the IGNORED base must not appear).
	require.NoError(t, m.AddTopologyGeneration(context.Background(),
		[]string{"IGNORED"}, []string{"g1", "g2", "g3", "g4", "g5", "g6"}))
	gens = m.fsm.PlacementGenerations()
	require.Len(t, gens, 3, "second growth appends exactly one generation")
	require.Equal(t, []string{"g1", "g2", "g3", "g4", "g5", "g6"}, gens[2].groupIDs)

	// Empty expanded set is rejected.
	require.Error(t, m.AddTopologyGeneration(context.Background(), []string{"g1"}, nil))
}
