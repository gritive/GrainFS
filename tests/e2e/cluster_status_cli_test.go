package e2e

import (
	"encoding/json"
	"os/exec"

	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClusterStatusCLIE2E verifies `grainfs cluster status` against both
// deployment shapes. After the DistributedBackend unification, mode is
// always "cluster"; the deployment shape is distinguished by peer count
// (singleton = 0 peers, cluster = N-1 peers).
//
// The cluster CLI uses the admin Unix socket (mode 0660 + admin-group)
// since v0.0.89; HTTP /api/cluster/status remains for the dashboard.
var _ = ginkgo.Describe("Cluster admin CLI status", func() {
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

			runClusterStatusCLICases(func() s3Target { return tgt })
		})
	}
})

func runClusterStatusCLICases(getTgt func() s3Target) {
	ginkgo.It("renders JSON", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		binary := getBinary()
		sock := tgt.adminSockPath()
		expectedPeers := 0
		if tgt.isCluster {
			expectedPeers = tgt.nodes - 1
		}

		out, err := exec.Command(binary, "cluster",
			"--endpoint", sock,
			"status", "--format", "json",
		).Output()
		require.NoError(t, err, "cluster status command must succeed")

		var status map[string]any
		require.NoError(t, json.Unmarshal(out, &status), "output must be valid JSON")

		assert.Equal(t, "cluster", status["mode"], "unified path must report mode=cluster")
		peers, _ := status["peers"].([]any)
		assert.Len(t, peers, expectedPeers, "peer count must match deployment shape")
	})

	ginkgo.It("renders text", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		binary := getBinary()
		sock := tgt.adminSockPath()

		out, err := exec.Command(binary, "cluster",
			"--endpoint", sock,
			"status", "--format", "text",
		).Output()
		require.NoError(t, err, "cluster status command must succeed")

		output := string(out)
		assert.Contains(t, output, "mode", "human-readable output must include mode")
		assert.Contains(t, output, "cluster", "unified path shows cluster mode")
	})
}
