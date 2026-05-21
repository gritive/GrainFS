package e2e

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// Cluster EC S3 op context exercises the clusterECS3OpContext helper
// (per-operation deadline regardless of parent context state).
var _ = ginkgo.Describe("Cluster EC S3 op context", func() {
	runClusterECS3OpContextCases()
})

func runClusterECS3OpContextCases() {
	ginkgo.It("gives expired parent contexts a fresh budget (ExpiredParentGetsFreshBudget)", func() {
		parent, cancelParent := context.WithCancel(context.Background())
		cancelParent()

		ctx, cancel := clusterECS3OpContext(parent, 50*time.Millisecond)
		ginkgo.DeferCleanup(cancel)

		gomega.Expect(ctx.Err()).NotTo(gomega.HaveOccurred())
		select {
		case <-ctx.Done():
			gomega.Expect(ctx.Err()).To(gomega.MatchError(context.DeadlineExceeded))
		case <-time.After(100 * time.Millisecond):
			ginkgo.Fail("operation context did not enforce its timeout")
		}
	})

	ginkgo.It("inherits cancellation from live parent contexts (LiveParentStillCancelsChild)", func() {
		parent, cancelParent := context.WithCancel(context.Background())
		ctx, cancel := clusterECS3OpContext(parent, time.Second)
		ginkgo.DeferCleanup(cancel)

		cancelParent()

		select {
		case <-ctx.Done():
			gomega.Expect(ctx.Err()).To(gomega.MatchError(context.Canceled))
		case <-time.After(100 * time.Millisecond):
			ginkgo.Fail("operation context did not inherit live parent cancellation")
		}
	})
}
