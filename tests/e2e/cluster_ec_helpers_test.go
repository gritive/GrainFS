package e2e

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/require"
)

// Cluster EC S3 op context exercises the clusterECS3OpContext helper
// (per-operation deadline regardless of parent context state).
var _ = ginkgo.Describe("Cluster EC S3 op context", func() {
	runClusterECS3OpContextCases()
})

func runClusterECS3OpContextCases() {
	ginkgo.It("gives expired parent contexts a fresh budget (ExpiredParentGetsFreshBudget)", func() {
		t := ginkgo.GinkgoTB()
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

	ginkgo.It("inherits cancellation from live parent contexts (LiveParentStillCancelsChild)", func() {
		t := ginkgo.GinkgoTB()
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
