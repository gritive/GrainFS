package serveruntime

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
)

// TestDecideDeferredSeed pins the Option B post-join verdict. The decision is a
// pure function of (expect-nodes flag, replicated shard-group count, live node
// count) — that purity is what makes deferred seeding leader-change-safe: any
// leader, original or one elected mid-bootstrap, computes the same verdict from
// replicated state with no persistent marker.
func TestDecideDeferredSeed(t *testing.T) {
	// For expectNodes=4, seedGroupCountForClusterSize(4) = 16 (4*4). EC width = 4.
	const expect4Target = 16

	tests := []struct {
		name           string
		expectNodes    int
		existingGroups int
		liveNodes      int
		want           deferredSeedDecision
	}{
		// Default behavior: unset / single-target clusters never defer.
		{"unset flag → passthrough", 0, 0, 1, seedPassthrough},
		{"expect=1 → passthrough", 1, 0, 1, seedPassthrough},

		// Deferred cluster, quorum not yet reached → suppress the per-join expand
		// so no partial-RF groups are created before the target size.
		{"deferred, solo genesis → suppress", 4, 0, 1, seedSuppress},
		{"deferred, 2 of 4 joined → suppress", 4, 0, 2, seedSuppress},
		{"deferred, 3 of 4 joined → suppress", 4, 0, 3, seedSuppress},

		// Quorum reached, batch not yet seeded → seed now.
		{"deferred, 4 of 4 joined, 0 groups → seedNow", 4, 0, 4, seedNow},

		// Leader-change / crash re-entry mid-seed: a partial batch (some groups
		// already proposed by a dead leader) at quorum must still seed the rest.
		// MissingSeedShardGroups makes the actual seed convergent; the verdict
		// must keep firing until the batch is complete.
		{"re-entry, partial batch at quorum → seedNow", 4, 7, 4, seedNow},
		{"re-entry, one short at quorum → seedNow", 4, expect4Target - 1, 4, seedNow},

		// Batch complete → passthrough so the normal per-join expand handles any
		// growth beyond the initial target size.
		{"batch complete → passthrough", 4, expect4Target, 4, seedPassthrough},
		{"complete + growth to 5 nodes → passthrough", 4, expect4Target, 5, seedPassthrough},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := decideDeferredSeed(tc.expectNodes, tc.existingGroups, tc.liveNodes)
			if got != tc.want {
				t.Fatalf("decideDeferredSeed(expect=%d, groups=%d, live=%d) = %d; want %d",
					tc.expectNodes, tc.existingGroups, tc.liveNodes, got, tc.want)
			}
		})
	}
}

// TestDecideDeferredSeed_NoDoubleSeedAfterComplete guards the idempotency
// invariant the lock-recheck relies on: once the batch is complete, the verdict
// is never seedNow again, so a late/overlapping post-join hook cannot re-seed.
func TestDecideDeferredSeed_NoDoubleSeedAfterComplete(t *testing.T) {
	target := seedGroupCountForClusterSize(4)
	for live := 4; live <= 8; live++ {
		if d := decideDeferredSeed(4, target, live); d != seedPassthrough {
			t.Fatalf("complete batch at live=%d returned %d; want seedPassthrough (no re-seed)", live, d)
		}
	}
}

// TestHandleDeferredSeed_WiringSuppressThenUniformSeed drives the actual seed
// wiring against a real leader MetaRaft — the part that broke twice in probes
// (the pure decision test never broke; the wiring did). It asserts the two
// invariants those failures violated:
//   - below quorum → suppressed, ZERO groups (no partial-RF batch created early)
//   - at quorum    → all initial groups seeded at uniform RF=N (no RF=1 group)
//
// plus idempotent re-entry (a leader-change re-call must not duplicate the batch).
func TestHandleDeferredSeed_WiringSuppressThenUniformSeed(t *testing.T) {
	ctx, state := storagePhasePrereqs(t)
	require.NoError(t, WaitForMetaRaftLeader(ctx, state.metaRaft, 5*time.Second))
	state.cfg.BootstrapExpectNodes = 4

	// liveNodes slice with self first (self ID/addr must match so it isn't
	// double-counted as a peer); peers fabricated — the FSM records the voter
	// IDs without requiring the peers to be live raft nodes.
	mkNodes := func(n int) []cluster.MetaNodeEntry {
		out := []cluster.MetaNodeEntry{{ID: state.nodeID, Address: state.raftAddr}}
		for i := 2; i <= n; i++ {
			out = append(out, cluster.MetaNodeEntry{ID: fmt.Sprintf("n%d", i), Address: fmt.Sprintf("127.0.0.1:70%02d", i)})
		}
		return out
	}

	// Below quorum (2 of 4): suppress, create NO groups. This is the regression
	// that produced partial-RF groups in the first probe (expand ran un-gated).
	handled, err := handleDeferredSeed(ctx, state, mkNodes(2))
	require.NoError(t, err)
	require.True(t, handled, "below quorum must be handled (suppressed), not passed to the per-join expand")
	require.Empty(t, state.metaRaft.FSM().ShardGroups(), "no groups may be seeded before the target node count is reached")

	// At quorum (4 of 4): seed all initial groups at uniform RF=4 (no RF=1).
	handled, err = handleDeferredSeed(ctx, state, mkNodes(4))
	require.NoError(t, err)
	require.True(t, handled)
	target := seedGroupCountForClusterSize(4)
	groups := state.metaRaft.FSM().ShardGroups()
	require.Len(t, groups, target, "deferred batch must seed exactly the target group count")
	for _, g := range groups {
		require.Lenf(t, g.PeerIDs, 4, "group %s must be RF=4 uniform — no RF=1 zero-redundancy group", g.ID)
	}

	// Idempotent re-entry: batch complete → passthrough, no duplicate/extra groups.
	handled, err = handleDeferredSeed(ctx, state, mkNodes(4))
	require.NoError(t, err)
	require.False(t, handled, "completed batch must pass through to the normal expand, not re-seed")
	require.Len(t, state.metaRaft.FSM().ShardGroups(), target, "re-entry must not duplicate or grow the batch")
}
