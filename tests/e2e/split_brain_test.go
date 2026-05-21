package e2e

import (
	"io"
	"net/http"
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		t := ginkgo.GinkgoTB()
		endpoint := getTgt().endpoint(0)
		resp, err := http.Get(endpoint + "/metrics")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Contains(t, string(body), "grainfs_split_brain_suspected",
			"/metrics must expose split brain indicator")
	})

	ginkgo.It("reports zero in steady state", func() {
		t := ginkgo.GinkgoTB()
		endpoint := getTgt().endpoint(0)
		resp, err := http.Get(endpoint + "/metrics")
		require.NoError(t, err)
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		for _, line := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(line, "grainfs_split_brain_suspected") && !strings.HasPrefix(line, "#") {
				assert.Contains(t, line, "0",
					"split_brain_suspected must be 0 in steady state, got: %q", line)
				return
			}
		}
		require.Fail(t, "grainfs_split_brain_suspected metric not found in /metrics output")
	})
}
