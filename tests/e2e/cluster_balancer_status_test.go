package e2e

import (
	"encoding/json"
	"os/exec"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
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
		tgt := getTgt()
		binary := getBinary()
		sock := tgt.adminSockPath()

		out, err := exec.Command(binary, "cluster",
			"--endpoint", sock,
			"balancer", "status", "--format", "json",
		).Output()
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "balancer status command must succeed")

		var b map[string]any
		gomega.Expect(json.Unmarshal(out, &b)).To(gomega.Succeed(), "output must be valid JSON")
		_, ok := b["available"]
		gomega.Expect(ok).To(gomega.BeTrue(), "available field expected: %v", b)
	})

	ginkgo.It("renders text", func() {
		tgt := getTgt()
		binary := getBinary()
		sock := tgt.adminSockPath()

		out, err := exec.Command(binary, "cluster",
			"--endpoint", sock,
			"balancer", "status",
		).Output()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		output := string(out)
		gomega.Expect(output).To(gomega.ContainSubstring("balancer:"))
	})
}
