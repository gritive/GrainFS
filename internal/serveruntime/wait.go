package serveruntime

import (
	"context"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/storage"
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

// waitForSnapshotBackendReady polls until ListAllObjects succeeds or the
// timeout elapses. The auto-snapshotter relies on this to delay its first
// sweep until the underlying backend has finished any restore/migration
// work that would otherwise produce empty snapshots.
func waitForSnapshotBackendReady(ctx context.Context, snapshotable storage.Snapshotable, timeout time.Duration) error {
	readyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		if _, err := snapshotable.ListAllObjects(); err == nil {
			return nil
		} else {
			lastErr = err
		}

		select {
		case <-readyCtx.Done():
			return fmt.Errorf("snapshot backend not ready after %s: %w", timeout, lastErr)
		case <-ticker.C:
		}
	}
}
