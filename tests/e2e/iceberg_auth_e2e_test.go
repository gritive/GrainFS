package e2e

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestIcebergE2E_NoAuth_Rejected: an unsigned http.Post to /iceberg/v1/namespaces
// must be rejected with 401 by the iceberg auth gate.
func TestIcebergE2E_NoAuth_Rejected(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		srv := startIAMTestServer(t)
		defer srv.Stop()

		resp, err := http.Post(srv.S3URL+"/iceberg/v1/namespaces", "application/json",
			bytes.NewReader([]byte(`{"namespace":["x"]}`)))
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})
}

// TestIcebergE2E_AfterBootstrap_Accepts: bootstrap an admin SA via UDS,
// sign a /iceberg/v1/config request with that key; the SigV4 verifier accepts.
func TestIcebergE2E_AfterBootstrap_Accepts(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		srv := startIAMTestServer(t)
		defer srv.Stop()

		client := newIcebergSigV4Client(t, srv.BootstrapAK, srv.BootstrapSK, "us-east-1")
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
			srv.S3URL+"/iceberg/v1/config?warehouse=warehouse", nil)
		require.NoError(t, err)
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.NotEqual(t, http.StatusUnauthorized, resp.StatusCode,
			"signed request should pass authn (got 401)")
		require.NotEqual(t, http.StatusForbidden, resp.StatusCode,
			"signed request should not be 403 either")
	})
}

// TestIcebergE2E_AuthFailures_Audited: 3 unsigned POSTs to /iceberg/v1/...
// should surface in the audit event log with non-empty Action and Reason.
func TestIcebergE2E_AuthFailures_Audited(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		srv := startIAMTestServer(t)
		defer srv.Stop()
		for i := 0; i < 3; i++ {
			resp, err := http.Post(srv.S3URL+"/iceberg/v1/warehouses", "application/json",
				bytes.NewReader([]byte(`{}`)))
			require.NoError(t, err)
			_ = resp.Body.Close()
		}
		time.Sleep(200 * time.Millisecond) // audit pipeline is async

		events := getEventLog(t, srv.S3URL)
		failures := 0
		for _, e := range events {
			action, _ := e["action"].(string)
			reason, _ := e["err_reason"].(string)
			authStatus, _ := e["auth_status"].(string)
			if authStatus == "deny" && action != "" && reason != "" {
				failures++
			}
		}
		require.GreaterOrEqual(t, failures, 3,
			"expected ≥3 iceberg auth failures audited with non-empty action+reason")
	})
}
