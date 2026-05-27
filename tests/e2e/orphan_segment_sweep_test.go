package e2e

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Orphan segment sweep", func() {
	ginkgo.Context("Cluster4Node", func() {
		ginkgo.It("deletes old orphan segments and increments metrics", func() {
			runOrphanSegmentSweepCases(ginkgo.GinkgoTB())
		})
	})
})

func runOrphanSegmentSweepCases(t testing.TB) {
	t.Helper()
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:         4,
		ScrubInterval: "500ms",
		ExtraArgs:     []string{"--scrub-orphan-age=1s", "--segment-gc-retention=0"},
		DisableNFS:    true,
		DisableNBD:    true,
	})

	// Create the bucket in cluster metadata so the scrubber's ListBuckets
	// call includes it and the segment walker is invoked for this bucket.
	const bucket = "orphan-sweep-test"
	ctx := context.Background()
	_, err := c.EnsureBucketWritable(ctx, bucket, 120*time.Second)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "bucket creation must succeed before seeding orphan")

	// Seed a fake orphan raw segment on node 0.
	// Path layout: <dataDir>/data/<bucket>/<key>_segments/<blobID>
	dataRoot := c.dataDirs[0]
	orphanPath := filepath.Join(dataRoot, "data", bucket, "orphan-key_segments", "fake-blob")
	gomega.Expect(os.MkdirAll(filepath.Dir(orphanPath), 0o755)).To(gomega.Succeed())
	gomega.Expect(os.WriteFile(orphanPath, []byte("orphan-content"), 0o644)).To(gomega.Succeed())

	// Backdate mtime past the (shortened) age gate so the walker considers
	// the file eligible immediately.
	past := time.Now().Add(-10 * time.Second)
	gomega.Expect(os.Chtimes(orphanPath, past, past)).To(gomega.Succeed())

	// Wait for 2 scrubber cycles (500ms × 2 + margin). The first cycle
	// tombstones the file; the second cycle deletes it.
	gomega.Eventually(func() bool {
		_, err := os.Stat(orphanPath)
		return os.IsNotExist(err)
	}, 15*time.Second, 200*time.Millisecond).Should(gomega.BeTrue(),
		"orphan segment should be deleted within 2 scrub cycles")

	// Assert the counter metric was incremented on at least one node.
	const metricName = "grainfs_scrub_orphan_segments_deleted_total"
	var metricLine string
	var metricFound bool

	for nodeIdx := range c.httpURLs {
		metricsURL := c.httpURLs[nodeIdx] + "/metrics"
		resp, httpErr := http.Get(metricsURL) //nolint:noctx
		if httpErr != nil {
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			continue
		}
		for _, line := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(line, "#") {
				continue
			}
			if strings.HasPrefix(line, metricName+" ") {
				metricLine = line
				metricFound = true
				break
			}
		}
		if metricFound {
			break
		}
	}

	gomega.Expect(metricFound).To(gomega.BeTrue(), "metric %q not found in /metrics output", metricName)
	gomega.Expect(metricLine).NotTo(gomega.Equal(metricName+" 0"),
		"metric should have incremented past 0")
}
