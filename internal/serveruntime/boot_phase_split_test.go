package serveruntime_test

import (
	"os"
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("R1 narrow boot phase ordering (run.go)", func() {
	var src string
	ginkgo.BeforeEach(func() {
		b, err := os.ReadFile("run.go")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		src = string(b)
	})
	idx := func(needle string) int { return strings.Index(src, needle) }

	ginkgo.It("populates the keeper before the gate", func() {
		gomega.Expect(idx("bootWALAndForwardersPart1(ctx, state)")).To(gomega.BeNumerically(">", 0))
		gomega.Expect(idx("WaitDEKReady(readyCtx")).To(gomega.BeNumerically(">", idx("bootWALAndForwardersPart1(ctx, state)")))
	})
	ginkgo.It("wraps the backend after routed storage exists", func() {
		gomega.Expect(idx("bootClusterCoordinatorRouting(state)")).To(gomega.BeNumerically(">", idx("bootOwnedGroupsAndEC(ctx, state")))
		gomega.Expect(idx("bootBackendWrap(ctx, state)")).To(gomega.BeNumerically(">", idx("bootClusterCoordinatorRouting(state)")))
		gomega.Expect(idx("bootBackendWrap(ctx, state)")).To(gomega.BeNumerically(">", idx("bootSnapshotAndApplyLoop(state)")))
	})
	ginkgo.It("removed the old late WaitDEKReady (only one gate call site)", func() {
		gomega.Expect(strings.Count(src, "WaitDEKReady(readyCtx")).To(gomega.Equal(1))
	})
	ginkgo.It("no longer opens the logical WAL inside bootWALAndForwardersPart1", func() {
		b, err := os.ReadFile("boot_phases_forwarders.go")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		fn := string(b)
		start := strings.Index(fn, "func bootWALAndForwardersPart1(")
		gomega.Expect(start).To(gomega.BeNumerically(">=", 0))
		end := strings.Index(fn[start+1:], "\nfunc ") + start + 1
		gomega.Expect(fn[start:end]).NotTo(gomega.ContainSubstring("wal.OpenEncrypted("))
	})
})

var _ = ginkgo.Describe("R-FSM-α boot reorder (run.go)", func() {
	var src string
	ginkgo.BeforeEach(func() {
		b, err := os.ReadFile("run.go")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		src = string(b)
	})
	idx := func(needle string) int { return strings.Index(src, needle) }

	ginkgo.It("defers bootShardService past WaitDEKReady", func() {
		gomega.Expect(idx("WaitDEKReady(readyCtx")).To(gomega.BeNumerically(">", 0))
		gomega.Expect(idx("bootShardService(ctx, state)")).To(gomega.BeNumerically(">", idx("WaitDEKReady(readyCtx")))
	})
	ginkgo.It("registers forward handlers after bootShardService", func() {
		gomega.Expect(idx("bootShardService(ctx, state)")).To(gomega.BeNumerically(">", 0))
		gomega.Expect(idx("bootRegisterForwardHandlers(state)")).To(gomega.BeNumerically(">", idx("bootShardService(ctx, state)")))
	})
	ginkgo.It("registers forward handlers before bootBalancerAndGossip", func() {
		gomega.Expect(idx("bootRegisterForwardHandlers(state)")).To(gomega.BeNumerically(">", 0))
		gomega.Expect(idx("bootBalancerAndGossip(ctx, state)")).To(gomega.BeNumerically(">", idx("bootRegisterForwardHandlers(state)")))
	})
	ginkgo.It("runs bootWALAndForwardersPart1 before WaitDEKReady", func() {
		gomega.Expect(idx("bootWALAndForwardersPart1(ctx, state)")).To(gomega.BeNumerically(">", 0))
		gomega.Expect(idx("bootWALAndForwardersPart1(ctx, state)")).To(gomega.BeNumerically("<", idx("WaitDEKReady(readyCtx")))
	})
	ginkgo.It("removed the old bootWALAndForwarders call site", func() {
		// The old name must not appear as a function call (open paren after the name).
		gomega.Expect(strings.Index(src, "bootWALAndForwarders(")).To(gomega.Equal(-1))
	})
})
