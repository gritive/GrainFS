// Request-ID middleware e2e (auth-redesign §5 T41).
//
// Verifies every public response carries X-GrainFS-Request-Id and that the
// header is dual-written to x-amz-request-id. /metrics is anonymous so no
// SigV4 signing is needed; the middleware runs first in the chain and is
// independent of route surface, so /metrics is a sufficient probe.
package e2e

import (
	"io"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const reqIDHeader = "X-GrainFS-Request-Id"

var _ = ginkgo.Describe("Request ID", func() {
	for _, tc := range []struct {
		name string
		mk   func() s3Target
	}{
		{name: "SingleNode", mk: newSingleNodeS3Target},
		{name: "Cluster4Node", mk: func() s3Target { return newSharedClusterS3Target(ginkgo.GinkgoTB()) }},
	} {
		tc := tc
		ginkgo.Context(tc.name, func() {
			var tgt s3Target

			ginkgo.BeforeEach(func() {
				tgt = tc.mk()
			})

			runRequestIDCases(func() s3Target { return tgt })
		})
	}
})

func runRequestIDCases(getTgt func() s3Target) {
	ginkgo.It("generates a request ID when absent", func() {
		t := ginkgo.GinkgoTB()
		endpoint := getTgt().endpoint(0)
		req, err := http.NewRequest(http.MethodGet, endpoint+"/metrics", nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer func() {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}()

		rid := resp.Header.Get(reqIDHeader)
		require.NotEmpty(t, rid, "server must generate X-GrainFS-Request-Id when absent")
		parsed, err := uuid.Parse(rid)
		require.NoError(t, err, "generated rid must be a valid UUID")
		assert.Equal(t, uuid.Version(7), parsed.Version(), "generated rid must be UUIDv7")

		// Dual-write to x-amz-request-id must match.
		assert.Equal(t, rid, resp.Header.Get("x-amz-request-id"),
			"x-amz-request-id must mirror X-GrainFS-Request-Id")
	})

	ginkgo.It("preserves an incoming request ID", func() {
		t := ginkgo.GinkgoTB()
		endpoint := getTgt().endpoint(0)
		const incoming = "client-rid-e2e-12345"
		req, err := http.NewRequest(http.MethodGet, endpoint+"/metrics", nil)
		require.NoError(t, err)
		req.Header.Set(reqIDHeader, incoming)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer func() {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}()

		assert.Equal(t, incoming, resp.Header.Get(reqIDHeader),
			"server must preserve client-supplied X-GrainFS-Request-Id")
		assert.Equal(t, incoming, resp.Header.Get("x-amz-request-id"),
			"x-amz-request-id must mirror the preserved rid")
	})

	ginkgo.It("generates a unique ID per request", func() {
		t := ginkgo.GinkgoTB()
		endpoint := getTgt().endpoint(0)
		// Two back-to-back unmarked requests must receive distinct rids.
		first := newRequestIDProbe(t, endpoint)
		second := newRequestIDProbe(t, endpoint)
		assert.NotEqual(t, first, second, "consecutive generated rids must differ")
	})
}

func newRequestIDProbe(t testing.TB, endpoint string) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, endpoint+"/metrics", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	rid := resp.Header.Get(reqIDHeader)
	require.NotEmpty(t, rid)
	return rid
}
