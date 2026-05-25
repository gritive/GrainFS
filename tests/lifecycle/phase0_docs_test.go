//go:build lifecycle

package lifecycle_test

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Phase 0 docs", func() {
	It("brings up a local node and runs the docs Phase 0 verify block", func() {
		h := NewHarness(GinkgoT())
		Expect(h.StartLocal()).To(Succeed())
		DeferCleanup(h.Stop)

		// Sanity: single-node serve reports mode=local.
		st, err := h.Status()
		Expect(err).NotTo(HaveOccurred())
		Expect(st.Mode).To(Equal("local"))

		// Read the docs and pull out Phase 0 blocks.
		md, err := os.ReadFile(repoRelPath("docs/operators/cluster-lifecycle.md"))
		Expect(err).NotTo(HaveOccurred())
		blocks, err := ExtractBlocks(string(md), "0")
		Expect(err).NotTo(HaveOccurred())
		Expect(blocks).NotTo(BeEmpty(), "no phase=0 blocks annotated in cluster-lifecycle.md")

		// Set env vars the docs reference.
		os.Setenv("GRAINFS_DATA", h.DataDir())
		os.Setenv("GRAINFS_PORT", h.portString())
		DeferCleanup(func() {
			os.Unsetenv("GRAINFS_DATA")
			os.Unsetenv("GRAINFS_PORT")
		})

		// Run every Phase 0 block. The verify block uses
		// `--no-sign-request` so no credentials are needed.
		for _, b := range blocks {
			Expect(RunBlock(Envsubst(b))).To(Succeed())
		}

		// Confirm the sentinel object lives in the default bucket.
		_, err = os.Stat(filepath.Join(h.DataDir(), "lifecycle-sentinel.txt"))
		Expect(err).NotTo(HaveOccurred(), "docs block did not create the local sentinel file")
	})
})
