//go:build compat

package compat

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestKEKEnvelopeV1Rejected asserts that in an N-1 + N mixed-version cluster,
// an admin KEK rotate trigger on the N (current) node is rejected with 503
// because the N-1 node does not advertise the kek_envelope_v1 capability.
//
// Skip conditions:
//   - COMPAT_PREV_BIN unset → prevBinary(t) marks intent; startCompatCluster skips.
//   - Phase A → Phase B migration not yet available → startCompatCluster always skips.
func TestKEKEnvelopeV1Rejected(t *testing.T) {
	prev := prevBinary(t)
	cur := getBinary()

	// node 0 = prev binary (seed/leader), node 1 = current binary (follower)
	c := startCompatCluster(t, []string{prev, cur})
	t.Cleanup(func() { c.Stop() })

	// POST /v1/encrypt/kek/rotate on the current node (node 1).
	// Body includes the correct confirm token so the request gets past input
	// validation (JSON decode + confirm check) and reaches the capability gate.
	// The gate returns 503 because node 0 (N-1 binary) does not advertise
	// kek_envelope_v1.
	body := []byte(`{"confirm":"rotate-now"}`)
	status, resp := postCompatAdminJSON(t, c.AdminSock(1), "/v1/encrypt/kek/rotate", body)
	require.Equal(t, http.StatusServiceUnavailable, status, "expected capability gate rejection (503), got: %s", resp)
	require.Contains(t, resp, "kek_envelope_v1", "response must name the missing capability")
}
