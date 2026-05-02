package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClusterECS3OpContext_ExpiredParentGetsFreshBudget(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()

	ctx, cancel := clusterECS3OpContext(parent, 50*time.Millisecond)
	defer cancel()

	require.NoError(t, ctx.Err())
	select {
	case <-ctx.Done():
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("operation context did not enforce its timeout")
	}
}

func TestClusterECS3OpContext_LiveParentStillCancelsChild(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, cancel := clusterECS3OpContext(parent, time.Second)
	defer cancel()

	cancelParent()

	select {
	case <-ctx.Done():
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("operation context did not inherit live parent cancellation")
	}
}
