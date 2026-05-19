package colimafixture

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

// TestColimaClusterFixtureBoots boots a 3-node grainfs cluster on the macOS
// host (no colima dependency) and verifies HTTP / responds on each node.
// This is the load-bearing smoke test for the fixture itself — it proves the
// boot sequence, .join-pending wiring, and admin SA bootstrap all work, so
// the colima cluster-mount tests (Tasks 15b/16/17) only need to consume the
// returned struct.
//
// Budget: <60s. Boot is ~10–20s; the test stops the cluster immediately.
func TestColimaClusterFixtureBoots(t *testing.T) {
	start := time.Now()
	c := StartCluster(t, Options{
		NumNodes:  3,
		EnableNFS: true,
		EnableNBD: true,
		EnableP9:  true,
	})
	bootDur := time.Since(start)
	t.Logf("cluster booted in %v (3 nodes, all protocols)", bootDur)

	if len(c.HTTPPorts) != 3 {
		t.Fatalf("expected 3 HTTP ports, got %d", len(c.HTTPPorts))
	}
	if c.AccessKey == "" || c.SecretKey == "" {
		t.Fatalf("expected non-empty admin AK/SK, got AK=%q SK=%q", c.AccessKey, c.SecretKey)
	}
	if c.LeaderIdx < 0 || c.LeaderIdx >= len(c.HTTPPorts) {
		t.Fatalf("LeaderIdx %d out of range", c.LeaderIdx)
	}
	t.Logf("leader idx=%d", c.LeaderIdx)

	// Verify each node serves HTTP /. Use a fresh deadline per node — we just
	// finished boot and waited for HTTP readiness in StartCluster, so this
	// should succeed immediately.
	for i, port := range c.HTTPPorts {
		url := fmt.Sprintf("http://127.0.0.1:%d/", port)
		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("node %d GET %s: %v", i, url, err)
		}
		_ = resp.Body.Close()
		// Any non-5xx status proves the listener and HTTP stack are alive.
		if resp.StatusCode >= 500 {
			t.Fatalf("node %d GET %s: status %d", i, url, resp.StatusCode)
		}
	}
}
