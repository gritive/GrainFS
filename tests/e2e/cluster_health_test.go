package e2e

import (
	"encoding/json"
	"os/exec"

	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClusterHealthCLIE2E verifies `grainfs cluster health` against both
// deployment shapes. Returns 200 and renders quorum + issues regardless
// of singleton vs cluster. With no configured peers the local-mode rule
// does not fire, so issues should be empty (apart from EC degraded if
// backend is degraded).
var _ = ginkgo.Describe("Cluster admin CLI health", func() {
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

			runClusterHealthCLICases(func() s3Target { return tgt })
		})
	}
})

func runClusterHealthCLICases(getTgt func() s3Target) {
	ginkgo.It("renders JSON", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		binary := getBinary()
		sock := tgt.adminSockPath()

		out, err := exec.Command(binary, "cluster",
			"--endpoint", sock,
			"health", "--format", "json",
		).Output()
		require.NoError(t, err, "cluster health command must succeed")

		var h map[string]any
		require.NoError(t, json.Unmarshal(out, &h), "output must be valid JSON")
		assert.Contains(t, []any{"cluster", "local"}, h["mode"])
		_, ok := h["quorum"].(map[string]any)
		assert.True(t, ok, "quorum object expected")
	})

	ginkgo.It("renders text", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		binary := getBinary()
		sock := tgt.adminSockPath()

		out, err := exec.Command(binary, "cluster",
			"--endpoint", sock,
			"health",
		).Output()
		require.NoError(t, err)

		output := string(out)
		assert.Contains(t, output, "mode:")
		assert.Contains(t, output, "ISSUES")
	})
}
