package e2e

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gritive/GrainFS/internal/clusteradmin"
	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// ec_redundancy_upgrade_test.go — e2e durability proof for the retroactive
// EC-redundancy-upgrade sweep (epic T0-T5).
//
// Scenario: an object written while the cluster was a single (genesis) node
// lands non-redundant (1+0) — its only copy lives on node 0. After the cluster
// grows to four nodes and forms redundant wide EC groups, a background scrubber
// sweep relocates that object into a redundant group, preserving its identity
// (body, ETag, LastModified, VersionID). The proof is that killing the original
// single-owner node 0 no longer loses the object: a pre-relocation 1+0 object
// would be unrecoverable, a post-relocation 2+2 object survives.
var _ = ginkgo.Describe("EC redundancy upgrade", func() {
	ginkgo.Context("genesis-single grows to cluster", func() {
		ginkgo.It("relocates a 1+0 object into a redundant group and survives owner kill", func() {
			runECRedundancyUpgradeSurvivesOwnerKill(ginkgo.GinkgoTB())
		})
	})
})

func runECRedundancyUpgradeSurvivesOwnerKill(t testing.TB) {
	t.Helper()

	// Start a GENESIS single node (no --bootstrap-expect-nodes): writes here
	// land non-redundant (1+0). Pre-allocate 4 port slots so addNode can grow
	// the cluster. The hardcoded "--scrub-interval 0" in startNode is overridden
	// by the trailing ExtraArgs (pflag last-value wins), and min-age 0 lets the
	// sweep relocate immediately instead of waiting out the 5m default gate.
	c := startMRCluster(t, 1, mrClusterOptions{
		FastBootstrap: true,
		MaxNodes:      4,
		ExtraArgs: []string{
			"--scrub-interval", "2s",
			"--ec-redundancy-upgrade",
			"--ec-redundancy-upgrade-min-age", "0",
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	ginkgo.DeferCleanup(cancel)

	const bucket = "ec-redundancy-upgrade"
	const key = "genesis-object"
	body := bytes.Repeat([]byte("redundancy-upgrade-payload-0123456789!"), 128) // ~4.75 KiB

	requireMRCreateBucketEventually(t, ctx, c, bucket)

	// Step 1: PUT the object on the genesis single node and capture identity.
	requireMRPutObjectFromAnyNodeEventually(t, ctx, c, bucket, key, body)
	before := requireHeadIdentity(t, ctx, c, bucket, key)
	t.Logf("genesis PUT ok: etag=%s versionID=%s lastModified=%s parity(initial)=%d",
		before.etag, before.versionID, before.lastModified, ecObjectParity(t, c, bucket, key))

	// Step 2: GROW to 4 nodes. seedGroupCountForClusterSize(4) = 16 groups,
	// which include redundant wide EC groups (one shard per node).
	for i := 0; i < 3; i++ {
		c.addNode(t)
	}
	waitForShardGroupCount(t, c.dataDirs[c.leaderIdx], 16, 90*time.Second)
	t.Logf("grew to 4 nodes; >=16 shard groups confirmed (redundant wide EC groups formed)")

	// Step 3: wait for the background sweep to relocate the genesis object.
	// Primary signal: the relocation counter increments on the node that ran
	// the sweep. We poll /metrics across all live nodes.
	waitForRelocationMetric(t, c, 1, 120*time.Second)
	t.Logf("redundancy-upgrade relocation metric observed (object relocated to a redundant group)")

	// Best-effort direct parity signal (the admin placement report's
	// ActualECParity is not wired in every build; treat >0 as confirming,
	// 0/unknown falls back to the kill-survival proxy in step 5).
	if p := ecObjectParity(t, c, bucket, key); p > 0 {
		t.Logf("placement report confirms redundant layout: parity=%d", p)
	} else {
		t.Logf("placement parity not directly observable (got %d); relying on metric + owner-kill survival proxy", p)
	}

	// Step 4: IDENTITY PRESERVED across the relocation.
	requireMRGetObjectFromAnyNodeEventually(t, ctx, c, bucket, key, body)
	after := requireHeadIdentity(t, ctx, c, bucket, key)
	gomega.Expect(after.etag).To(gomega.Equal(before.etag), "ETag must be unchanged after relocation")
	gomega.Expect(after.versionID).To(gomega.Equal(before.versionID), "VersionID must be unchanged after relocation")
	gomega.Expect(after.lastModified).To(gomega.Equal(before.lastModified),
		"LastModified must be unchanged after relocation (identity-preserving relocate)")
	t.Logf("identity preserved: etag/versionID/lastModified all unchanged")

	// Step 5: KILL the original single-owner node 0. Pre-relocation this would
	// lose the object (its only 1+0 copy lived there); post-relocation the
	// redundant group reconstructs it from the surviving nodes.
	gomega.Expect(c.procs[0]).NotTo(gomega.BeNil(), "node 0 process must exist")
	t.Logf("killing original owner node 0 at %s", c.httpURLs[0])
	terminateProcess(c.procs[0])
	c.procs[0] = nil

	// The object must remain readable from a surviving node (eventually, after
	// any re-election). requireMRGetObjectFromAnyNodeEventually already skips
	// nodes that error, so a dead node 0 is tolerated.
	readCtx, readCancel := context.WithTimeout(context.Background(), 120*time.Second)
	ginkgo.DeferCleanup(readCancel)
	requireMRGetObjectFromAnyNodeEventually(t, readCtx, c, bucket, key, body)
	t.Logf("DURABILITY PROOF: object survived owner-node-0 kill — it was relocated to a redundant group")
}

// objectIdentity captures the HEAD-observable identity fields that the
// redundancy-upgrade relocate must preserve.
type objectIdentity struct {
	etag         string
	versionID    string
	lastModified string
}

// requireHeadIdentity HEADs the object from any live node and returns its
// identity fields, retrying until a node answers.
func requireHeadIdentity(t testing.TB, ctx context.Context, c *mrCluster, bucket, key string) objectIdentity {
	t.Helper()
	var id objectIdentity
	var lastErr error
	gomega.Eventually(func() bool {
		for _, endpoint := range c.liveURLs() {
			cli := ecS3Client(endpoint, c.accessKey, c.secretKey)
			hctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			out, err := cli.HeadObject(hctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			cancel()
			if err != nil {
				lastErr = err
				continue
			}
			id = objectIdentity{}
			if out.ETag != nil {
				id.etag = *out.ETag
			}
			if out.VersionId != nil {
				id.versionID = *out.VersionId
			}
			if out.LastModified != nil {
				// Render at second granularity (S3 Last-Modified has no
				// sub-second precision on the wire).
				id.lastModified = out.LastModified.UTC().Format(time.RFC3339)
			}
			return id.etag != ""
		}
		return false
	}).WithTimeout(60*time.Second).WithPolling(2*time.Second).
		Should(gomega.BeTrue(), "HEAD never returned object identity: lastErr=%v", lastErr)
	return id
}

// ecObjectParity queries the admin placement report for the object's actual EC
// parity. Returns -1 when the report is unavailable/unwired. Used as a
// best-effort direct signal; the metric + owner-kill proxy is authoritative.
func ecObjectParity(t testing.TB, c *mrCluster, bucket, key string) int {
	t.Helper()
	sock := filepath.Join(c.dataDirs[c.leaderIdx], "admin.sock")
	cli := clusteradmin.NewClient(sock)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	report, err := cli.Placement(ctx, clusteradmin.PlacementOptions{Bucket: bucket, Key: key, Limit: 10})
	if err != nil || report == nil {
		return -1
	}
	for _, e := range report.Details {
		if e.Key == key {
			return int(e.ActualECParity)
		}
	}
	return -1
}

// waitForRelocationMetric polls /metrics on every live node until the
// redundancy-upgrade relocation counter reaches at least want on any node.
func waitForRelocationMetric(t testing.TB, c *mrCluster, want int, timeout time.Duration) {
	t.Helper()
	const metricName = "grainfs_ec_redundancy_upgrade_relocated_total"
	const failedMetric = "grainfs_ec_redundancy_upgrade_failed_total"
	var lastSeen float64 = -1
	var lastFailed float64 = -1
	gomega.Eventually(func() bool {
		for _, endpoint := range c.liveURLs() {
			v, ok := scrapeCounter(endpoint+"/metrics", metricName)
			if !ok {
				t.Logf("scrape %s: metric %q not found", endpoint, metricName)
				continue
			}
			if f, ok := scrapeCounter(endpoint+"/metrics", failedMetric); ok && f > lastFailed {
				lastFailed = f
			}
			if v > lastSeen {
				lastSeen = v
			}
			if int(v) >= want {
				return true
			}
		}
		return false
	}).WithTimeout(timeout).WithPolling(time.Second).
		Should(gomega.BeTrue(),
			"relocation metric %q never reached %d (highest seen=%v, failed counter=%v)",
			metricName, want, lastSeen, lastFailed)
}

// scrapeCounter fetches a Prometheus text endpoint and returns the value of the
// named (unlabeled) counter, or (0,false) if absent/unreachable.
func scrapeCounter(metricsURL, metricName string) (float64, bool) {
	resp, err := http.Get(metricsURL) //nolint:noctx
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, metricName+" ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		v, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return 0, false
		}
		return v, true
	}
	return 0, false
}
