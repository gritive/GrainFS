package server

import "testing"

// TestClusterHealth_WireGolden locks the JSON byte shape of
// GET /v1/cluster/health against a deterministic fixture.
func TestClusterHealth_WireGolden(t *testing.T) {
	cli, base := newGoldenFixtureServer(t)
	body := fetchBody(t, cli, base+"/v1/cluster/health")
	assertGoldenJSON(t, "cluster_health.golden.json", body)
}
