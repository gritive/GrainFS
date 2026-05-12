package server

import "testing"

// TestClusterPlacement_WireGolden locks the JSON byte shape of
// GET /v1/cluster/placement?bucket=b against a deterministic fixture.
func TestClusterPlacement_WireGolden(t *testing.T) {
	cli, base := newGoldenFixtureServer(t)
	body := fetchBody(t, cli, base+"/v1/cluster/placement?bucket=b&limit=10")
	assertGoldenJSON(t, "cluster_placement.golden.json", body)
}
