package server

import "testing"

// TestBalancerStatus_WireGolden locks the JSON byte shape of
// GET /v1/cluster/balancer/status against a deterministic fixture. Includes
// one node with a zero JoinedAt to lock omitempty behavior — Task 9 swaps
// the producer to adminapi.BalancerStatus and must preserve the same shape.
func TestBalancerStatus_WireGolden(t *testing.T) {
	cli, base := newGoldenFixtureServer(t)
	body := fetchBody(t, cli, base+"/v1/cluster/balancer/status")
	assertGoldenJSON(t, "balancer_status.golden.json", body)
}
