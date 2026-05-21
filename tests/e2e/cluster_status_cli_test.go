package e2e

import (
	"encoding/json"
	"os/exec"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
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
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "cluster status command must succeed")

		var status map[string]any
		gomega.Expect(json.Unmarshal(out, &status)).To(gomega.Succeed(), "output must be valid JSON")

		gomega.Expect(status["mode"]).To(gomega.Equal("cluster"), "unified path must report mode=cluster")
		peers, _ := status["peers"].([]any)
		gomega.Expect(peers).To(gomega.HaveLen(expectedPeers), "peer count must match deployment shape")
	})

	ginkgo.It("renders text", func() {
		tgt := getTgt()
		binary := getBinary()
		sock := tgt.adminSockPath()

		out, err := exec.Command(binary, "cluster",
			"--endpoint", sock,
			"status", "--format", "text",
		).Output()
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "cluster status command must succeed")

		output := string(out)
		gomega.Expect(output).To(gomega.ContainSubstring("mode"), "human-readable output must include mode")
		gomega.Expect(output).To(gomega.ContainSubstring("cluster"), "unified path shows cluster mode")
	})
}
