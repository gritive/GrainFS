package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"syscall"
	"testing"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Cluster remove peer", func() {
	ginkgo.Context("Cluster3Node", func() {
		var c *e2eCluster

		ginkgo.BeforeEach(func() {
			t := ginkgo.GinkgoTB()

			c = startE2ECluster(t, e2eClusterOptions{
				Nodes:      3,
				Mode:       ClusterModeDynamicJoin,
				ClusterKey: "E2E-REMOVE-PEER-KEY",
				AccessKey:  "rm-ak",
				SecretKey:  "rm-sk",
				LogPrefix:  "grainfs-remove-peer",
			})
		})

		// Spins up a 3-node cluster, kills a follower, and verifies the operator
		// can evict it via /api/cluster/remove-peer. Validates the full chain:
		// pre-flight allows it, joint consensus commits, status reflects the
		// shrunk voter set, and audit event surfaces.
		ginkgo.It("evicts a dead follower", func() {
			t := ginkgo.GinkgoTB()

			leaderIdx := c.leaderIdx
			gomega.Expect(leaderIdx).To(gomega.BeNumerically(">=", 0), "harness must have identified leader")

			// Pick the first non-leader.
			followerIdx := -1
			for i := range c.procs {
				if i != leaderIdx {
					followerIdx = i
					break
				}
			}
			gomega.Expect(followerIdx).To(gomega.BeNumerically(">=", 0))
			// /api/cluster/status.peers reports node IDs, and remove-peer accepts the
			// same identifier.
			deadID := c.nodeID(followerIdx)
			leaderURL := c.httpURLs[leaderIdx]

			// Wait for the dynamic-join membership to settle. Leader's Peers() excludes
			// self, so a 3-node cluster should report 2 remote voters once joins commit.
			var lastStatus map[string]any
			settled := false
			for i := 0; i < 120; i++ {
				s := getStatusJSON(t, leaderURL)
				lastStatus = s
				voters := stringList(s["peers"])
				if len(voters) == 2 && containsString(voters, deadID) {
					settled = true
					break
				}
				time.Sleep(500 * time.Millisecond)
			}
			if !settled {
				t.Fatalf("leader never observed 2 remote voters incl %s; last status: %+v", deadID, lastStatus)
			}

			// Kill the follower hard so it never rejoins during the test.
			gomega.Expect(c.procs[followerIdx].Process.Signal(syscall.SIGKILL)).To(gomega.Succeed())
			_ = c.procs[followerIdx].Wait()
			c.procs[followerIdx] = nil

			// Happy path with no --force: LivePeers reports all metaRaft voters as
			// alive (PR-D will refine this with real liveness). Pre-flight math —
			// 3 voters → 2, alive_after=2, new_quorum=2 — passes; engine commits.
			body, _ := json.Marshal(map[string]any{"id": deadID, "force": false})
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			ginkgo.DeferCleanup(cancel)
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, leaderURL+"/api/cluster/remove-peer", bytes.NewReader(body))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			respBody, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK), "remove-peer must succeed, body=%s", string(respBody))

			// After remove, leader's remote voter set shrinks from 2 to 1, no longer contains the removed id.
			gomega.Eventually(func() bool {
				s := getStatusJSON(t, leaderURL)
				voters := stringList(s["peers"])
				return len(voters) == 1 && !containsString(voters, deadID)
			}, 30*time.Second, 500*time.Millisecond).Should(gomega.BeTrue(), "voter set must shrink to 1 remote without the removed id")

			// Audit event must surface in the event log.
			events := getEventLog(t, leaderURL)
			found := false
			for _, e := range events {
				if action, _ := e["action"].(string); action == "cluster-remove-peer" {
					found = true
					break
				}
			}
			gomega.Expect(found).To(gomega.BeTrue(), "cluster-remove-peer event must appear in /api/eventlog")
		})
	})
})

func getStatusJSON(t testing.TB, base string) map[string]any {
	t.Helper()
	resp, err := http.Get(base + "/api/cluster/status")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()
	out := map[string]any{}
	gomega.Expect(json.NewDecoder(resp.Body).Decode(&out)).To(gomega.Succeed())
	return out
}

func getEventLog(t testing.TB, base string) []map[string]any {
	t.Helper()
	resp, err := http.Get(base + "/api/eventlog?since=300&limit=200")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()
	var events []map[string]any
	gomega.Expect(json.NewDecoder(resp.Body).Decode(&events)).To(gomega.Succeed())
	return events
}

func stringList(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, x := range arr {
		if s, ok := x.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func containsString(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
