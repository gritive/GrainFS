package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// clusterConfigGetResponse mirrors adminapi.ClusterConfigResponse for the JSON
// returned by GET /v1/cluster/config.
type clusterConfigGetResponse struct {
	Rev         uint64            `json:"rev"`
	UpdatedAtMs int64             `json:"updated_at_unix_ms"`
	Effective   map[string]any    `json:"effective"`
	Source      map[string]string `json:"source"`
}

// adminUDSClient returns an http.Client wired to dial the admin Unix domain
// socket at <dataDir>/admin.sock.
func adminUDSClient(dataDir string) *http.Client {
	socket := filepath.Join(dataDir, "admin.sock")
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socket)
			},
		},
		Timeout: 5 * time.Second,
	}
}

// SetClusterConfig PATCHes /v1/cluster/config on the given node's admin UDS
// and returns the post-apply rev. Body is a map of kebab-case keys (plus the
// optional "reset_keys"). Duration-typed keys take a string ("5s", "100ms").
func SetClusterConfig(t *testing.T, dataDir string, body map[string]any) uint64 {
	t.Helper()
	buf, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPatch, "http://unix/v1/cluster/config", bytes.NewReader(buf))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := adminUDSClient(dataDir).Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	require.Equalf(t, http.StatusOK, resp.StatusCode,
		"PATCH /v1/cluster/config: %d: %s", resp.StatusCode, respBody)
	var out struct {
		Rev uint64 `json:"rev"`
	}
	require.NoError(t, json.Unmarshal(respBody, &out))
	return out.Rev
}

// GetClusterConfig GETs /v1/cluster/config on the given node's admin UDS and
// returns the parsed response. Used by hot-reload tests to verify followers
// observe a leader-applied change.
func GetClusterConfig(t *testing.T, dataDir string) clusterConfigGetResponse {
	t.Helper()
	resp, err := adminUDSClient(dataDir).Get("http://unix/v1/cluster/config")
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	require.Equalf(t, http.StatusOK, resp.StatusCode,
		"GET /v1/cluster/config: %d: %s", resp.StatusCode, respBody)
	var out clusterConfigGetResponse
	require.NoError(t, json.Unmarshal(respBody, &out))
	return out
}
