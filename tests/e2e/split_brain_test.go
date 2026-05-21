package e2e

import (
	"io"
	"net/http"
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// TestSplitBrainE2E asserts the grainfs_split_brain_suspected metric is
// exposed and reads zero on the shared fixtures (no split observed). Shared
// single + shared cluster — Prometheus scrape is read-only, no state
// mutation across sub-tests.
var _ = ginkgo.Describe("Split brain metrics", func() {
	for _, tc := range []struct {
		name string
		mk   func() s3Target
	}{
		{name: "SingleNode", mk: newSingleNodeS3Target},
		{name: "Cluster4Node", mk: func() s3Target { return newSharedClusterS3Target(ginkgo.GinkgoTB()) }},
	} {
		tc := tc
		ginkgo.Context(tc.name, func() {
			var tgt s3Target

			ginkgo.BeforeEach(func() {
				tgt = tc.mk()
			})

			runSplitBrainCases(func() s3Target { return tgt })
		})
	}
})

func runSplitBrainCases(getTgt func() s3Target) {
	ginkgo.It("exposes the split brain indicator metric", func() {
		endpoint := getTgt().endpoint(0)
		resp, err := http.Get(endpoint + "/metrics")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer resp.Body.Close()
		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))

		body, err := io.ReadAll(resp.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(string(body)).To(gomega.ContainSubstring("grainfs_split_brain_suspected"),
			"/metrics must expose split brain indicator")
	})

	ginkgo.It("reports zero in steady state", func() {
		endpoint := getTgt().endpoint(0)
		resp, err := http.Get(endpoint + "/metrics")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, line := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(line, "grainfs_split_brain_suspected") && !strings.HasPrefix(line, "#") {
				gomega.Expect(line).To(gomega.ContainSubstring("0"),
					"split_brain_suspected must be 0 in steady state, got: %q", line)
				return
			}
		}
		ginkgo.Fail("grainfs_split_brain_suspected metric not found in /metrics output")
	})
}
