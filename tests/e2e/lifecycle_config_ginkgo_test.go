//go:build integration

package e2e

import (
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// TestLifecycleConfigGinkgo is the standard go-test entry point for the
// Ginkgo specs in this file. Allows `go test ./tests/e2e/` to run them
// without the separate `ginkgo` CLI.
func TestLifecycleConfigGinkgo(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Lifecycle Config e2e (PoC)")
}

var _ = ginkgo.Describe("Lifecycle config", func() {
	ginkgo.Context("Sanity", func() {
		ginkgo.It("runs a no-op Ginkgo spec successfully", func() {
			gomega.Expect(1 + 1).To(gomega.Equal(2))
		})

		ginkgo.It("can call testing.TB adapter from GinkgoT()", func() {
			tb := ginkgo.GinkgoT()
			gomega.Expect(tb).NotTo(gomega.BeNil())
			// Helper t.Helper() style call doesn't crash via adapter
			tb.Helper()
		})
	})
})
