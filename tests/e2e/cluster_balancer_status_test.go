package e2e

import (
	"encoding/json"
	"os/exec"

	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClusterBalancerStatusCLIE2E verifies `cluster balancer status` on
// both single-node and 4-node fixtures. Balancer may be active or not
// depending on harness; both should produce structured output.
var _ = ginkgo.Describe("Cluster admin CLI balancer status", func() {
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

			runClusterBalancerStatusCLICases(func() s3Target { return tgt })
		})
	}
})

func runClusterBalancerStatusCLICases(getTgt func() s3Target) {
	ginkgo.It("renders JSON", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		binary := getBinary()
		sock := tgt.adminSockPath()

		out, err := exec.Command(binary, "cluster",
			"--endpoint", sock,
			"balancer", "status", "--format", "json",
		).Output()
		require.NoError(t, err, "balancer status command must succeed")

		var b map[string]any
		require.NoError(t, json.Unmarshal(out, &b), "output must be valid JSON")
		_, ok := b["available"]
		assert.True(t, ok, "available field expected: %v", b)
	})

	ginkgo.It("renders text", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		binary := getBinary()
		sock := tgt.adminSockPath()

		out, err := exec.Command(binary, "cluster",
			"--endpoint", sock,
			"balancer", "status",
		).Output()
		require.NoError(t, err)

		output := string(out)
		assert.Contains(t, output, "balancer:")
	})
}
