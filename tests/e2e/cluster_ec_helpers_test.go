package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestClusterECS3OpContextE2E exercises the clusterECS3OpContext helper
// (per-operation deadline regardless of parent context state). Both
// sub-tests are pure unit checks on the helper function — no fixture is
// touched — but we run them under SingleNode/Cluster4Node sub-test
// branches for shape parity with the rest of the suite.
func TestClusterECS3OpContextE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		_ = newSingleNodeS3Target()
		runClusterECS3OpContextCases(t)
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		_ = newSharedClusterS3Target(t)
		runClusterECS3OpContextCases(t)
	})
}

func runClusterECS3OpContextCases(t *testing.T) {
	t.Helper()

	t.Run("ExpiredParentGetsFreshBudget", func(t *testing.T) {
		parent, cancelParent := context.WithCancel(context.Background())
		cancelParent()

		ctx, cancel := clusterECS3OpContext(parent, 50*time.Millisecond)
		defer cancel()

		require.NoError(t, ctx.Err())
		select {
		case <-ctx.Done():
			require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
		case <-time.After(100 * time.Millisecond):
			require.Fail(t, "operation context did not enforce its timeout")
		}
	})

	t.Run("LiveParentStillCancelsChild", func(t *testing.T) {
		parent, cancelParent := context.WithCancel(context.Background())
		ctx, cancel := clusterECS3OpContext(parent, time.Second)
		defer cancel()

		cancelParent()

		select {
		case <-ctx.Done():
			require.ErrorIs(t, ctx.Err(), context.Canceled)
		case <-time.After(100 * time.Millisecond):
			require.Fail(t, "operation context did not inherit live parent cancellation")
		}
	})
}
