package serveruntime

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
)

// indexGroupID returns the zero-padded ID for index group i of n
// ("index-00".."index-15" at n=16). Width = len(strconv.Itoa(n-1)) so
// lexicographic sort == numeric shard order, which makes IndexGroups()
// (sorted by ID) place the group for hash%n==i at slot i. A naive
// "index-%d" breaks at n>=10 ("index-10" < "index-2"). This is the SINGLE
// source of the padding rule.
func indexGroupID(i, n int) string {
	if n < 1 {
		n = 1
	}
	width := len(strconv.Itoa(n - 1))
	return fmt.Sprintf("index-%0*d", width, i)
}

// SeedInitialIndexGroups proposes index-00..N-1 IndexGroupEntry registrations via
// meta-raft at genesis. Mirrors SeedInitialShardGroups (wait-for-leader then
// propose per group), with three deliberate departures:
//
//   - RF=N: every index group's PeerIDs is the FULL boot peer set (self + peers,
//     address→nodeID resolved like group-0). Reads are local on every node. NOT
//     PickVoters triplets (that is a deferred RF=3 path).
//   - Fixed N: the count is the configured value, never cluster-size-derived, and
//     this seed is never re-run by the data-group join expansion (hash%N stability).
//   - Fail-hard: a partial seed (M<N) would leave hash%N buckets with no group —
//     boot-time corruption of the routing invariant. Return on the first propose
//     failure (unlike the data-group seed, which logs and continues).
//
// indexGroupCount<=1 seeds nothing: the meta-FSM single-shard path stays.
// normalGroupVoters is accepted for signature parity with SeedInitialShardGroups
// but never affects PeerIDs under RF=N.
func SeedInitialIndexGroups(
	ctx context.Context,
	metaRaft *cluster.MetaRaft,
	selfNodeID string,
	selfAddr string,
	peers []string,
	indexGroupCount int,
	normalGroupVoters int,
) error {
	if indexGroupCount <= 1 {
		return nil
	}

	bootstrapCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	if err := WaitForMetaRaftLeader(bootstrapCtx, metaRaft, 15*time.Second); err != nil {
		return err
	}

	// RF=N: full cluster peer set (self + peers), address→nodeID resolved via the
	// FSM address book exactly like group-0. NOT SeedShardGroupVoters (PickVoters +
	// single-node multi-slot replication must not leak in).
	voters := seedShardGroupPeerIDs(selfNodeID, selfAddr, peers, liveNonRevokedNodes(metaRaft.FSM()))

	for i := 0; i < indexGroupCount; i++ {
		id := indexGroupID(i, indexGroupCount)
		igCtx, igCancel := context.WithTimeout(bootstrapCtx, 5*time.Second)
		err := metaRaft.ProposeIndexGroup(igCtx, cluster.IndexGroupEntry{
			ID:      id,
			PeerIDs: voters,
		})
		igCancel()
		if err != nil {
			return fmt.Errorf("seed index group %s: %w", id, err)
		}
	}
	return nil
}

// indexGroupSource is the use-site interface for WaitForIndexGroupCount: the
// MetaFSM satisfies it via IndexGroups().
type indexGroupSource interface {
	IndexGroups() []cluster.IndexGroupEntry
}

// WaitForIndexGroupCount polls until at least `want` index groups are observable
// or the timeout elapses. Mirrors WaitForShardGroupCount; used at bootstrap to
// confirm SeedInitialIndexGroups proposals applied before the index router flips on.
func WaitForIndexGroupCount(ctx context.Context, src indexGroupSource, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(src.IndexGroups()) >= want {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("only %d/%d index groups visible after %s", len(src.IndexGroups()), want, timeout)
}
