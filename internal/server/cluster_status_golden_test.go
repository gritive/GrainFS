package server

import "testing"

// TestClusterStatus_WireGolden locks the JSON byte shape of
// GET /v1/cluster/status against a deterministic fixture. Re-run with
// -update-golden to refresh the captured baseline after an intentional
// wire change.
func TestClusterStatus_WireGolden(t *testing.T) {
	cli, base := newGoldenFixtureServer(t)
	body := fetchBody(t, cli, base+"/v1/cluster/status")
	assertGoldenJSON(t, "cluster_status.golden.json", body)
}
