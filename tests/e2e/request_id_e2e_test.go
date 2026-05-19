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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const reqIDHeader = "X-GrainFS-Request-Id"

func TestRequestIDE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runRequestIDCases(t, newSingleNodeS3Target())
	})

	t.Run("Cluster4Node", func(t *testing.T) {
		runRequestIDCases(t, newSharedClusterS3Target(t))
	})
}

func runRequestIDCases(t *testing.T, tgt s3Target) {
	endpoint := tgt.endpoint(0)

	t.Run("GeneratesIfAbsent", func(t *testing.T) {
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

	t.Run("PreservesIncoming", func(t *testing.T) {
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

	t.Run("UniquePerRequest", func(t *testing.T) {
		// Two back-to-back unmarked requests must receive distinct rids.
		first := newRequestIDProbe(t, endpoint)
		second := newRequestIDProbe(t, endpoint)
		assert.NotEqual(t, first, second, "consecutive generated rids must differ")
	})
}

func newRequestIDProbe(t *testing.T, endpoint string) string {
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
