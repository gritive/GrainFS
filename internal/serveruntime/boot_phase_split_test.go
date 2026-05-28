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
		gomega.Expect(idx("bootWALAndForwarders(ctx, state)")).To(gomega.BeNumerically(">", 0))
		gomega.Expect(idx("WaitDEKReady(readyCtx")).To(gomega.BeNumerically(">", idx("bootWALAndForwarders(ctx, state)")))
	})
	ginkgo.It("opens the logical WAL after the gate", func() {
		gomega.Expect(idx("bootLogicalWALOpen(ctx, state)")).To(gomega.BeNumerically(">", idx("WaitDEKReady(readyCtx")))
	})
	ginkgo.It("wraps the backend (wal consumer) after the logical WAL open", func() {
		gomega.Expect(idx("bootBackendWrap(ctx, state)")).To(gomega.BeNumerically(">", idx("bootLogicalWALOpen(ctx, state)")))
	})
	ginkgo.It("removed the old late WaitDEKReady (only one gate call site)", func() {
		gomega.Expect(strings.Count(src, "WaitDEKReady(readyCtx")).To(gomega.Equal(1))
	})
	ginkgo.It("no longer opens the logical WAL inside bootWALAndForwarders", func() {
		b, err := os.ReadFile("boot_phases_forwarders.go")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		fn := string(b)
		start := strings.Index(fn, "func bootWALAndForwarders(")
		gomega.Expect(start).To(gomega.BeNumerically(">=", 0))
		end := strings.Index(fn[start+1:], "\nfunc ") + start + 1
		gomega.Expect(fn[start:end]).NotTo(gomega.ContainSubstring("wal.OpenEncrypted("))
	})
})
