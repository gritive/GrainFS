package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/onsi/gomega"
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
			DisableKeepAlives: true,
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
func SetClusterConfig(t testing.TB, dataDir string, body map[string]any) uint64 {
	t.Helper()
	buf, err := json.Marshal(body)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req, err := http.NewRequest(http.MethodPatch, "http://unix/v1/cluster/config", bytes.NewReader(buf))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Set("Content-Type", "application/json")

	resp, err := adminUDSClient(dataDir).Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK),
		"PATCH /v1/cluster/config: %d: %s", resp.StatusCode, respBody)
	var out struct {
		Rev uint64 `json:"rev"`
	}
	gomega.Expect(json.Unmarshal(respBody, &out)).To(gomega.Succeed())
	return out.Rev
}

// patchSnapshotInterval PATCHes /v1/cluster/config on the given node's admin
// UDS to set "snapshot-interval" to dur. dur="0s" disables the auto-snapshot
// loop (used by e2e harnesses for determinism); a non-zero duration ("100ms",
// "5s") enables it. PATCH is Raft-replicated, so calling it once per cluster
// on any voter's admin UDS is sufficient.
func patchSnapshotInterval(t testing.TB, dataDir string, dur string) {
	t.Helper()
	gomega.Expect(patchSnapshotIntervalM(dataDir, dur)).To(gomega.Succeed(), "PATCH snapshot-interval")
}

// patchSnapshotIntervalM is the TestMain-friendly variant of
// patchSnapshotInterval — no *testing.T because TestMain doesn't have one.
// Returns an error instead of failing the test directly.
func patchSnapshotIntervalM(dataDir string, dur string) error {
	body, err := json.Marshal(map[string]any{"snapshot-interval": dur})
	if err != nil {
		return fmt.Errorf("marshal patch body: %w", err)
	}
	req, err := http.NewRequest(http.MethodPatch, "http://unix/v1/cluster/config", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build PATCH request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := adminUDSClient(dataDir).Do(req)
	if err != nil {
		return fmt.Errorf("PATCH /v1/cluster/config: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		buf, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PATCH /v1/cluster/config status %d: %s", resp.StatusCode, string(buf))
	}
	return nil
}

// GetClusterConfig GETs /v1/cluster/config on the given node's admin UDS and
// returns the parsed response. Used by hot-reload tests to verify followers
// observe a leader-applied change.
func GetClusterConfig(t testing.TB, dataDir string) clusterConfigGetResponse {
	t.Helper()
	resp, err := adminUDSClient(dataDir).Get("http://unix/v1/cluster/config")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK),
		"GET /v1/cluster/config: %d: %s", resp.StatusCode, respBody)
	var out clusterConfigGetResponse
	gomega.Expect(json.Unmarshal(respBody, &out)).To(gomega.Succeed())
	return out
}
