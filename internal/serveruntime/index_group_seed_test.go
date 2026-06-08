package serveruntime

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestSeedInitialIndexGroups_CountAndIDs pins the N=4 happy path (zero-padded
// width 1) and the gated N<=1 case (seed nothing — the meta-FSM single-shard
// path stays).
func TestSeedInitialIndexGroups_CountAndIDs(t *testing.T) {
	ctx, state := storagePhasePrereqs(t)
	require.NoError(t, WaitForMetaRaftLeader(ctx, state.metaRaft, 5*time.Second))

	require.NoError(t, SeedInitialIndexGroups(ctx, state.metaRaft, state.nodeID, state.raftAddr, nil, 4, 1))
	ids := make([]string, 0)
	for _, g := range state.metaRaft.FSM().IndexGroups() {
		ids = append(ids, g.ID)
	}
	require.ElementsMatch(t, []string{"index-0", "index-1", "index-2", "index-3"}, ids) // N=4 -> width 1

	// N=1 seeds nothing — fresh leader MetaRaft so prior seed doesn't leak.
	ctx2, state2 := storagePhasePrereqs(t)
	require.NoError(t, WaitForMetaRaftLeader(ctx2, state2.metaRaft, 5*time.Second))
	require.NoError(t, SeedInitialIndexGroups(ctx2, state2.metaRaft, state2.nodeID, state2.raftAddr, nil, 1, 1))
	require.Empty(t, state2.metaRaft.FSM().IndexGroups()) // N=1 seeds nothing
}

// TestSeedInitialIndexGroups_N16_PaddedOrdering pins the load-bearing invariant:
// zero-padded IDs make lexicographic sort == numeric shard order, so
// IndexGroups()[i] is the group for hash%N==i.
func TestSeedInitialIndexGroups_N16_PaddedOrdering(t *testing.T) {
	ctx, state := storagePhasePrereqs(t)
	require.NoError(t, WaitForMetaRaftLeader(ctx, state.metaRaft, 5*time.Second))

	require.NoError(t, SeedInitialIndexGroups(ctx, state.metaRaft, state.nodeID, state.raftAddr, nil, 16, 1))
	got := state.metaRaft.FSM().IndexGroups() // sorted by ID
	require.Len(t, got, 16)
	for i := range got {
		require.Equalf(t, indexGroupID(i, 16), got[i].ID, "shard %d must map to sorted IndexGroups()[%d]", i, i)
	}
	require.Equal(t, "index-00", got[0].ID)
	require.Equal(t, "index-15", got[15].ID)
}

// TestSeedInitialIndexGroups_RFisN pins RF=N: every index group's PeerIDs is the
// FULL boot peer set (all nodes are voters in every index group), NOT a
// PickVoters triplet.
func TestSeedInitialIndexGroups_RFisN(t *testing.T) {
	ctx, state := storagePhasePrereqs(t)
	require.NoError(t, WaitForMetaRaftLeader(ctx, state.metaRaft, 5*time.Second))

	peers := []string{"n2", "n3"} // peers arg (excludes self, which is state.nodeID == "n1")
	require.NoError(t, SeedInitialIndexGroups(ctx, state.metaRaft, state.nodeID, state.raftAddr, peers, 4, 1))
	for _, g := range state.metaRaft.FSM().IndexGroups() {
		require.ElementsMatch(t, []string{"n1", "n2", "n3"}, g.PeerIDs, "RF=N: every index group has the full peer set")
	}
}

// TestSeedInitialIndexGroups_ExemptFromJoinExpand is the regression guard for the
// load-bearing "fixed N, exempt from data-group join expansion" invariant. After
// seeding index groups, driving the data-group expand path (expandShardGroupsForJoinedNode)
// must leave IndexGroups() count unchanged while ShardGroups() grows. This is
// behavioral, not a grep-comment: if someone later adds an IndexGroups() read or
// write to the expand path, this test breaks.
func TestSeedInitialIndexGroups_ExemptFromJoinExpand(t *testing.T) {
	ctx, state := storagePhasePrereqs(t)
	require.NoError(t, WaitForMetaRaftLeader(ctx, state.metaRaft, 5*time.Second))

	require.NoError(t, SeedInitialIndexGroups(ctx, state.metaRaft, state.nodeID, state.raftAddr, nil, 4, 1))
	require.Len(t, state.metaRaft.FSM().IndexGroups(), 4)
	before := len(state.metaRaft.FSM().ShardGroups())

	// Default BootstrapExpectNodes (0) → handleDeferredSeed passthrough →
	// MissingSeedShardGroups seeds data groups. The expand reads/writes ONLY
	// ShardGroups() (boot_phases_forwarders.go expandShardGroupsForJoinedNode +
	// MissingSeedShardGroups), so it must not perturb IndexGroups(). In a 1-node
	// test cluster the data-group expand seeds group-0 then errors on a single-peer
	// non-group-0 (PickVoters → empty peers); the error is a harness artifact of the
	// data-group path, irrelevant to the index-group exemption we assert here.
	_ = expandShardGroupsForJoinedNode(ctx, state, state.nodeID)

	require.Len(t, state.metaRaft.FSM().IndexGroups(), 4, "index groups are exempt from data-group join expansion (fixed N)")
	require.Greater(t, len(state.metaRaft.FSM().ShardGroups()), before, "data-group expand must have seeded shard groups")
}

// WaitForIndexGroupCount is exercised here to pin its poll-until-count semantics
// against an already-seeded FSM (>= want returns nil; timeout returns error).
func TestWaitForIndexGroupCount(t *testing.T) {
	ctx, state := storagePhasePrereqs(t)
	require.NoError(t, WaitForMetaRaftLeader(ctx, state.metaRaft, 5*time.Second))
	require.NoError(t, SeedInitialIndexGroups(ctx, state.metaRaft, state.nodeID, state.raftAddr, nil, 4, 1))

	require.NoError(t, WaitForIndexGroupCount(ctx, state.metaRaft.FSM(), 4, 2*time.Second))
	require.Error(t, WaitForIndexGroupCount(context.Background(), state.metaRaft.FSM(), 5, 200*time.Millisecond))
}
