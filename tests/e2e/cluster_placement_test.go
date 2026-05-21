package e2e

import (
	"os/exec"
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClusterPlacementCLIE2E exercises the `grainfs cluster placement`
// admin CLI. Shared single + shared cluster fixtures — the helper accepts
// either the fallback "single-node mode" / "no shard groups" message or
// the topology-derived placement table, so the same assertions hold on
// both fixtures.
var _ = ginkgo.Describe("Cluster placement CLI", func() {
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

			runClusterPlacementCLICases(func() s3Target { return tgt })
		})
	}
})

func runClusterPlacementCLICases(getTgt func() s3Target) {
	ginkgo.It("renders fallback or placement table without a bucket", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		binary := getBinary()
		sock := tgt.adminSockPath()

		out, err := exec.Command(binary, "cluster",
			"--endpoint", sock,
			"placement",
		).Output()
		require.NoError(t, err, "placement command must succeed")

		output := string(out)
		hasFallback := false
		for _, want := range []string{"single-node mode", "no shard groups configured", "SHARD GROUPS", "Desired policy:"} {
			if strings.Contains(output, want) {
				hasFallback = true
				break
			}
		}
		assert.True(t, hasFallback, "expected one of fallback or table render; got: %q", output)
	})

	ginkgo.It("renders fallback or bucket status for an unknown bucket", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		binary := getBinary()
		sock := tgt.adminSockPath()

		out, err := exec.Command(binary, "cluster",
			"--endpoint", sock,
			"placement", "no-such-bucket",
		).Output()
		require.NoError(t, err)

		output := string(out)
		assert.True(t,
			strings.Contains(output, "not assigned") ||
				strings.Contains(output, "single-node mode") ||
				strings.Contains(output, "no shard groups configured") ||
				strings.Contains(output, "Bucket: no-such-bucket"),
			"expected not-assigned or fallback; got: %q", output)
	})
}
