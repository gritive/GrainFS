package e2e

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Raft snapshot admin", func() {
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

			runRaftSnapshotAdminCases(func() s3Target { return tgt })
		})
	}
})

func runRaftSnapshotAdminCases(getTgt func() s3Target) {
	ginkgo.It("triggers a snapshot and reports status and metrics", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		endpoint := tgt.endpoint(0)
		_ = tgt.uniqueBucket(t, "raftsnap")

		resp, err := http.Post(endpoint+"/admin/raft/snapshot", "application/json", nil) //nolint:noctx
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer resp.Body.Close()
		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))

		var trigger struct {
			Index     uint64 `json:"index"`
			Term      uint64 `json:"term"`
			SizeBytes int    `json:"size_bytes"`
		}
		gomega.Expect(json.NewDecoder(resp.Body).Decode(&trigger)).To(gomega.Succeed())
		gomega.Expect(trigger.Index).NotTo(gomega.BeZero())
		gomega.Expect(trigger.Term).NotTo(gomega.BeZero())
		gomega.Expect(trigger.SizeBytes).To(gomega.BeNumerically(">", 0))

		statusResp, err := http.Get(endpoint + "/admin/raft/snapshot") //nolint:noctx
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer statusResp.Body.Close()
		gomega.Expect(statusResp.StatusCode).To(gomega.Equal(http.StatusOK))

		var status struct {
			Available bool   `json:"available"`
			Index     uint64 `json:"index"`
			Term      uint64 `json:"term"`
			SizeBytes int    `json:"size_bytes"`
		}
		gomega.Expect(json.NewDecoder(statusResp.Body).Decode(&status)).To(gomega.Succeed())
		gomega.Expect(status.Available).To(gomega.BeTrue())
		gomega.Expect(status.Index).To(gomega.Equal(trigger.Index))
		gomega.Expect(status.Term).To(gomega.Equal(trigger.Term))
		gomega.Expect(status.SizeBytes).To(gomega.Equal(trigger.SizeBytes))

		metricsResp, err := http.Get(endpoint + "/metrics") //nolint:noctx
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer metricsResp.Body.Close()
		gomega.Expect(metricsResp.StatusCode).To(gomega.Equal(http.StatusOK))
		body, err := io.ReadAll(metricsResp.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		metrics := string(body)
		gomega.Expect(metrics).To(gomega.ContainSubstring("grainfs_raft_snapshot_trigger_total"))
		gomega.Expect(metrics).To(gomega.ContainSubstring("grainfs_raft_snapshot_last_index"))
		gomega.Expect(metrics).To(gomega.ContainSubstring("grainfs_raft_snapshot_last_size_bytes"))
	})
}
