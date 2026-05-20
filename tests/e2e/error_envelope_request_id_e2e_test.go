// Error envelope request_id propagation e2e (auth-redesign §5 T42).
//
// Verifies that the X-GrainFS-Request-Id header set by WithRequestID (T41)
// is propagated INTO the response body for both S3 XML and Iceberg JSON
// error envelopes. Clients can correlate failures with server-side audit
// records without trusting only the response header.
//
// Coverage:
//   - S3 XML: unsigned GET → SigV4 rejection → <RequestId> embedded in body
//   - Iceberg JSON: unsigned /iceberg/v1/config → 401 → "request_id" embedded
//   - Generated-rid path: omit the incoming header, ensure the server-
//     generated rid appears in the body AND matches the response header.
package e2e

import (
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestErrorEnvelopeRequestIDE2E covers §5 T42 on single + cluster targets.
func TestErrorEnvelopeRequestIDE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runErrorEnvelopeRequestIDCases(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runErrorEnvelopeRequestIDCases(t, newSharedClusterS3Target(t))
	})
}

func runErrorEnvelopeRequestIDCases(t *testing.T, tgt s3Target) {
	endpoint := tgt.endpoint(0)

	t.Run("S3_PreservesIncomingRid", func(t *testing.T) {
		const incoming = "rid-test-s3"
		// Unsigned GET on a non-existent bucket — SigV4 gate produces an XML
		// auth error envelope via writeXMLError. We don't care WHICH XML
		// error is returned; only that the rid was propagated into the body.
		req, err := http.NewRequest(http.MethodGet, endpoint+"/no-such-bucket-rid-test/", nil)
		require.NoError(t, err)
		req.Header.Set(reqIDHeader, incoming)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		require.GreaterOrEqual(t, resp.StatusCode, 400, "expected an error response")
		assert.Equal(t, incoming, resp.Header.Get(reqIDHeader),
			"server must preserve client-supplied X-GrainFS-Request-Id")

		var env struct {
			XMLName   xml.Name `xml:"Error"`
			Code      string   `xml:"Code"`
			RequestID string   `xml:"RequestId"`
		}
		require.NoError(t, xml.Unmarshal(body, &env), "body must be S3 XML error: %s", body)
		assert.Equal(t, incoming, env.RequestID,
			"S3 XML <RequestId> must echo X-GrainFS-Request-Id; body=%s", body)
	})

	t.Run("S3_GeneratesRidWhenAbsent", func(t *testing.T) {
		// Omit the incoming header — server generates a UUIDv7; the rid in the
		// response body must match the rid in the response header.
		req, err := http.NewRequest(http.MethodGet, endpoint+"/no-such-bucket-rid-gen/", nil)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		hdrRid := resp.Header.Get(reqIDHeader)
		require.NotEmpty(t, hdrRid, "server must generate rid when absent")

		var env struct {
			XMLName   xml.Name `xml:"Error"`
			RequestID string   `xml:"RequestId"`
		}
		require.NoError(t, xml.Unmarshal(body, &env), "body must be S3 XML error: %s", body)
		assert.Equal(t, hdrRid, env.RequestID,
			"S3 XML <RequestId> must match X-GrainFS-Request-Id header (generated path)")
	})

	t.Run("Iceberg_PreservesIncomingRid", func(t *testing.T) {
		const incoming = "rid-test-iceberg"
		// Unsigned Iceberg request — SigV4 gate rejects → writeIcebergError
		// remaps to 401 JSON envelope.
		req, err := http.NewRequest(http.MethodGet,
			endpoint+"/iceberg/v1/config?warehouse=rid-test-warehouse", nil)
		require.NoError(t, err)
		req.Header.Set(reqIDHeader, incoming)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		require.GreaterOrEqual(t, resp.StatusCode, 400, "expected an error response")
		assert.Equal(t, incoming, resp.Header.Get(reqIDHeader),
			"server must preserve client-supplied X-GrainFS-Request-Id")
		require.Contains(t, resp.Header.Get("Content-Type"), "application/json",
			"iceberg error envelope must be JSON")

		var env map[string]any
		require.NoError(t, json.Unmarshal(body, &env),
			"body must be Iceberg JSON error: %s", body)
		gotRid, _ := env["request_id"].(string)
		assert.Equal(t, incoming, gotRid,
			"Iceberg JSON request_id must echo X-GrainFS-Request-Id; body=%s", body)
	})

	t.Run("Iceberg_GeneratesRidWhenAbsent", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet,
			endpoint+"/iceberg/v1/config?warehouse=rid-test-warehouse-gen", nil)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		hdrRid := resp.Header.Get(reqIDHeader)
		require.NotEmpty(t, hdrRid, "server must generate rid when absent")
		require.True(t, strings.Contains(resp.Header.Get("Content-Type"), "application/json"),
			"iceberg error envelope must be JSON")

		var env map[string]any
		require.NoError(t, json.Unmarshal(body, &env),
			"body must be Iceberg JSON error: %s", body)
		gotRid, _ := env["request_id"].(string)
		assert.Equal(t, hdrRid, gotRid,
			"Iceberg JSON request_id must match X-GrainFS-Request-Id header (generated path)")
	})
}
