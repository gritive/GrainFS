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

	"github.com/stretchr/testify/require"
)

// TestOrphanSegmentSweepE2E exercises the cluster scrubber's orphan-segment
// sweep path. Single-node scrubber is covered separately; the cluster path
// has a different walker dispatch, so this test set has a Cluster4Node-only
// branch today. Future single-node analogue can drop in as a sibling
// t.Run("SingleNode", ...).
func TestOrphanSegmentSweepE2E(t *testing.T) {
	t.Run("Cluster4Node", func(t *testing.T) {
		runOrphanSegmentSweepCases(t)
	})
}

func runOrphanSegmentSweepCases(t *testing.T) {
	t.Helper()
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:         4,
		ScrubInterval: "500ms",
		ExtraArgs:     []string{"--scrub-orphan-age=1s"},
		DisableNFS:    true,
		DisableNBD:    true,
	})

	// Create the bucket in cluster metadata so the scrubber's ListBuckets
	// call includes it and the segment walker is invoked for this bucket.
	const bucket = "orphan-sweep-test"
	ctx := context.Background()
	c.GrantAdminOnBuckets(bucket)
	_, err := waitForWritableEndpoint(ctx, c.httpURLs, 120*time.Second, 5*time.Second, time.Second,
		func(attemptCtx context.Context, endpoint string) error {
			return tryCreateBucket(attemptCtx, ecS3Client(endpoint, c.accessKey, c.secretKey), bucket)
		})
	require.NoError(t, err, "bucket creation must succeed before seeding orphan")

	// Seed a fake orphan raw segment on node 0.
	// Path layout: <dataDir>/data/<bucket>/<key>_segments/<blobID>
	dataRoot := c.dataDirs[0]
	orphanPath := filepath.Join(dataRoot, "data", bucket, "orphan-key_segments", "fake-blob")
	require.NoError(t, os.MkdirAll(filepath.Dir(orphanPath), 0o755))
	require.NoError(t, os.WriteFile(orphanPath, []byte("orphan-content"), 0o644))

	// Backdate mtime past the (shortened) age gate so the walker considers
	// the file eligible immediately.
	past := time.Now().Add(-10 * time.Second)
	require.NoError(t, os.Chtimes(orphanPath, past, past))

	// Wait for 2 scrubber cycles (500ms × 2 + margin). The first cycle
	// tombstones the file; the second cycle deletes it.
	require.Eventually(t, func() bool {
		_, err := os.Stat(orphanPath)
		return os.IsNotExist(err)
	}, 15*time.Second, 200*time.Millisecond,
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
		resp.Body.Close()
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

	require.True(t, metricFound, "metric %q not found in /metrics output", metricName)
	require.NotEqual(t, metricName+" 0", metricLine,
		"metric should have incremented past 0")
}
