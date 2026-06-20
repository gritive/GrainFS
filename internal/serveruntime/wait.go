package serveruntime

import (
	"context"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
)

// WaitForMetaRaftLeader polls until the meta-raft node sees any leader
// (self or remote) or the timeout elapses. Returns nil as soon as a
// leader is visible; ctx cancellation is honored.
func WaitForMetaRaftLeader(ctx context.Context, metaRaft *cluster.MetaRaft, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if metaRaft.IsLeader() || metaRaft.Node().LeaderID() != "" {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("meta-raft leader not visible after %s", timeout)
}

// WaitForShardGroupCount polls until at least `want` shard groups are
// observable through the source, or the timeout elapses. Used during
// bootstrap to confirm SeedInitialShardGroups proposals applied before
// flipping the router into "explicit assignments only" mode.
func WaitForShardGroupCount(ctx context.Context, src cluster.ShardGroupSource, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(src.ShardGroups()) >= want {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("only %d/%d shard groups visible after %s", len(src.ShardGroups()), want, timeout)
}
