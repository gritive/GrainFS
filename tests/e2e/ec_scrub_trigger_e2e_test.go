//go:build !race
// +build !race

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

func adminUnixHTTPClient(socketPath string) *http.Client {
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socketPath)
			},
		},
	}
}

// EC scrub trigger exercises the POST /v1/scrub admin endpoint:
//
//   - FlowsThroughCluster: trigger replicates via meta-raft, every node
//     creates a session for the same SessionID, EC resolver routes the
//     bucket to its owning group's BadgerDB, at least one peer reports
//     Checked > 0 in the aggregated stat. Repair-on-corrupt-shard is
//     covered by cluster_scrubber_test.go.
//   - DedupHitReturnsExistingSession: a second identical POST returns the
//     same SessionID with created=false (LookupDedup short-circuit).
var _ = ginkgo.Describe("EC scrub trigger", func() {
	ginkgo.Context("Cluster3Node", func() {
		var c *e2eCluster

		ginkgo.BeforeEach(func() {
			c = startE2ECluster(ginkgo.GinkgoTB(), e2eClusterOptions{
				Nodes: 3, Mode: ClusterModeStaticPeers, LogPrefix: "ec-scrub-trigger",
				DisableNFS: true, DisableNBD: true,
			})
		})

		ginkgo.It("flows through the cluster", func() {
			runECScrubTriggerFlowsThroughCluster(ginkgo.GinkgoTB(), c)
		})

		ginkgo.It("deduplicates identical trigger requests", func() {
			runECScrubTriggerDedupHitReturnsExistingSession(ginkgo.GinkgoTB(), c)
		})
	})
})

func runECScrubTriggerFlowsThroughCluster(t testing.TB, c *e2eCluster) {
	t.Helper()
	ctx := context.Background()

	const bucket = "ec-test"
	_, err := c.EnsureBucketWritable(ctx, bucket, 120*time.Second)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for i := 0; i < 5; i++ {
		payload := bytes.Repeat([]byte{byte('a' + i)}, 4096)
		key := fmt.Sprintf("k-%d", i)
		_, err := waitForWritableEndpoint(ctx, c.httpURLs, 120*time.Second, 5*time.Second, time.Second, func(attemptCtx context.Context, endpoint string) error {
			return tryPutObject(attemptCtx, ecS3Client(endpoint, c.accessKey, c.secretKey), bucket, key, payload)
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	time.Sleep(2 * time.Second)

	body, _ := json.Marshal(map[string]any{"bucket": bucket, "scope": "full"})
	var httpCli *http.Client
	var resp *http.Response
	gomega.Eventually(func() bool {
		for _, dir := range c.dataDirs {
			candidate := adminUnixHTTPClient(filepath.Join(dir, "admin.sock"))
			r, postErr := candidate.Post("http://unix/v1/scrub", "application/json", bytes.NewReader(body))
			if postErr != nil {
				continue
			}
			if r.StatusCode == http.StatusCreated || r.StatusCode == http.StatusOK {
				httpCli = candidate
				resp = r
				return true
			}
			r.Body.Close()
		}
		return false
	}).WithTimeout(30*time.Second).WithPolling(500*time.Millisecond).Should(gomega.BeTrue(), "POST /v1/scrub must reach the meta-Raft leader")
	var sr map[string]any
	gomega.Expect(json.NewDecoder(resp.Body).Decode(&sr)).To(gomega.Succeed())
	ginkgo.DeferCleanup(resp.Body.Close)
	sessionID, _ := sr["session_id"].(string)
	gomega.Expect(sessionID).NotTo(gomega.BeEmpty())

	gomega.Eventually(func() bool {
		r, err := httpCli.Get("http://unix/v1/scrub/jobs/" + sessionID)
		if err != nil {
			return false
		}
		defer r.Body.Close()
		var info map[string]any
		if err := json.NewDecoder(r.Body).Decode(&info); err != nil {
			return false
		}
		status, _ := info["status"].(string)
		return status == "done"
	}).WithTimeout(60*time.Second).WithPolling(1*time.Second).Should(gomega.BeTrue(), "scrub session must reach done within 60s")

	r, err := httpCli.Get("http://unix/v1/scrub/jobs/" + sessionID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(r.Body.Close)
	var info map[string]any
	gomega.Expect(json.NewDecoder(r.Body).Decode(&info)).To(gomega.Succeed())
	t.Logf("session %s: bucket=%v scope=%v status=%v checked=%v detected=%v repaired=%v partial=%v",
		sessionID, info["bucket"], info["scope"], info["status"],
		info["checked"], info["detected"], info["repaired"], info["partial"])

	checked, ok := info["checked"].(float64)
	gomega.Expect(ok).To(gomega.BeTrue(), "checked must decode as a JSON number")
	gomega.Expect(info["status"]).To(gomega.Equal("done"), "session must reach done")
	gomega.Expect(info["bucket"]).To(gomega.Equal(bucket), "bucket field must round-trip")
	gomega.Expect(checked).To(gomega.BeNumerically(">", float64(0)), "EC resolver and aggregation path must report checked objects")
}

func runECScrubTriggerDedupHitReturnsExistingSession(t testing.TB, c *e2eCluster) {
	t.Helper()

	// Use a distinct bucket so dedup short-circuit is not satisfied by the
	// session from FlowsThroughCluster running first on this fixture.
	httpCli := adminUnixHTTPClient(filepath.Join(c.dataDirs[0], "admin.sock"))
	body, _ := json.Marshal(map[string]any{"bucket": "dedup-bk", "scope": "full"})

	first, err := httpCli.Post("http://unix/v1/scrub", "application/json", bytes.NewReader(body))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var firstResp map[string]any
	decodeScrubTriggerResp(t, first, &firstResp)
	gomega.Expect(firstResp["created"].(bool)).To(gomega.BeTrue(), "first call: created=true expected")
	firstID := firstResp["session_id"].(string)

	second, err := httpCli.Post("http://unix/v1/scrub", "application/json", bytes.NewReader(body))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var secondResp map[string]any
	decodeScrubTriggerResp(t, second, &secondResp)
	gomega.Expect(secondResp["created"].(bool)).To(gomega.BeFalse(), "second call: dedup hit must return Created=false")
	gomega.Expect(secondResp["session_id"].(string)).To(gomega.Equal(firstID), "dedup must return same SessionID")
}

func decodeScrubTriggerResp(t testing.TB, resp *http.Response, out *map[string]any) {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(resp.Body.Close()).To(gomega.Succeed())
	gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusCreated), "body: %s", string(body))
	gomega.Expect(json.Unmarshal(body, out)).To(gomega.Succeed())
}
