package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// startFakeClusterConfigUDS spins a fake admin UDS server that serves a fixed
// JSON body on GET /v1/cluster/config and returns the socket path. Used by the
// cobra RunE smoke tests below; wire-shape coverage lives in
// internal/clusteradmin/cluster_config_client_test.go.
func startFakeClusterConfigUDS(t *testing.T, body map[string]any) string {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "ccfg-")
	require.NoError(t, err)
	sock := filepath.Join(d, "admin.sock")
	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	})
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = ln.Close()
		_ = os.RemoveAll(d)
	})
	return sock
}

// recordedPatch captures the last PATCH /v1/cluster/config body and
// If-Match-Rev header for the cobra RunE smoke tests.
type recordedPatch struct {
	mu         sync.Mutex
	body       []byte
	ifMatchRev string
}

func (r *recordedPatch) LastPatchBody() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return string(r.body)
}

func (r *recordedPatch) LastIfMatchRev() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ifMatchRev
}

// startFakePatchUDS spins a fake admin UDS server that records the last PATCH
// /v1/cluster/config body + If-Match-Rev header, replies 200 with {"rev":N}.
func startFakePatchUDS(t *testing.T, replyRev uint64) (string, *recordedPatch) {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "ccfg-patch-")
	require.NoError(t, err)
	sock := filepath.Join(d, "admin.sock")
	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)

	rec := &recordedPatch{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, _ := io.ReadAll(r.Body)
		rec.mu.Lock()
		rec.body = body
		rec.ifMatchRev = r.Header.Get("If-Match-Rev")
		rec.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"rev": replyRev})
	})
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = ln.Close()
		_ = os.RemoveAll(d)
	})
	return sock, rec
}

func sampleConfigBody() map[string]any {
	return map[string]any{
		"rev":                7,
		"updated_at_unix_ms": int64(1_700_000_000_000),
		"effective": map[string]any{
			"balancer-imbalance-trigger-pct": 25.5,
			"balancer-imbalance-stop-pct":    10.0,
			"alert-webhook":                  "https://x.example",
		},
		"source": map[string]string{
			"balancer-imbalance-trigger-pct": "explicit",
			"balancer-imbalance-stop-pct":    "default",
			"alert-webhook":                  "explicit",
		},
	}
}

// The following tests exercise the cobra RunE wrappers so that the cmd→
// clusteradmin wiring + tabwriter rendering is covered end-to-end. Wire-shape
// assertions (method, path, body, header) live in clusteradmin.

func newClusterTestCmd(sock string) *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Flags().String("endpoint", sock, "")
	cmd.Flags().Bool("json", false, "")
	cmd.Flags().Uint64("if-match-rev", 0, "")
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetContext(context.Background())
	return cmd
}

func TestClusterConfigShowCmd_RunE(t *testing.T) {
	sock := startFakeClusterConfigUDS(t, sampleConfigBody())
	cmd := newClusterTestCmd(sock)
	require.NoError(t, clusterConfigShowCmd.RunE(cmd, nil))
	out := cmd.OutOrStdout().(*bytes.Buffer).String()
	require.Contains(t, out, "REV: 7")
	require.Contains(t, out, "balancer-imbalance-trigger-pct")
	require.Contains(t, out, "explicit")
}

func TestClusterConfigGetCmd_RunE(t *testing.T) {
	sock := startFakeClusterConfigUDS(t, sampleConfigBody())
	cmd := newClusterTestCmd(sock)
	require.NoError(t, clusterConfigGetCmd.RunE(cmd, []string{"balancer-imbalance-trigger-pct"}))
	require.Contains(t, cmd.OutOrStdout().(*bytes.Buffer).String(), "25.5")
}

func TestClusterConfigDiffCmd_RunE(t *testing.T) {
	sock := startFakeClusterConfigUDS(t, sampleConfigBody())
	cmd := newClusterTestCmd(sock)
	require.NoError(t, clusterConfigDiffCmd.RunE(cmd, nil))
	out := cmd.OutOrStdout().(*bytes.Buffer).String()
	require.Contains(t, out, "alert-webhook")
	require.NotContains(t, out, "balancer-imbalance-stop-pct")
}

func TestClusterConfigSetCmd_RunE(t *testing.T) {
	sock, rec := startFakePatchUDS(t, 9)
	cmd := newClusterTestCmd(sock)
	require.NoError(t, clusterConfigSetCmd.RunE(cmd, []string{"balancer-enabled=true"}))
	require.JSONEq(t, `{"balancer-enabled":true}`, rec.LastPatchBody())
}

func TestClusterConfigResetCmd_RunE(t *testing.T) {
	sock, rec := startFakePatchUDS(t, 9)
	cmd := newClusterTestCmd(sock)
	require.NoError(t, clusterConfigResetCmd.RunE(cmd, []string{"balancer-enabled"}))
	require.JSONEq(t, `{"reset_keys":["balancer-enabled"]}`, rec.LastPatchBody())
}

func TestClusterConfigSetCmd_InvalidKV(t *testing.T) {
	// kv parser rejects values without "=" before any HTTP call happens.
	sock, _ := startFakePatchUDS(t, 9)
	cmd := newClusterTestCmd(sock)
	err := clusterConfigSetCmd.RunE(cmd, []string{"no-equals-sign"})
	require.Error(t, err)
}

func TestClusterConfigCmds_RunE_EndpointError(t *testing.T) {
	// Each RunE propagates the clusterEndpointFromCmd error when --endpoint
	// is empty. Covers the single error-return branch in every RunE closure.
	emptyCmd := func() *cobra.Command {
		cmd := &cobra.Command{}
		cmd.Flags().String("endpoint", "", "")
		cmd.Flags().Bool("json", false, "")
		cmd.Flags().Uint64("if-match-rev", 0, "")
		cmd.SetContext(context.Background())
		return cmd
	}
	cases := []struct {
		name string
		run  func(*cobra.Command) error
	}{
		{"show", func(c *cobra.Command) error { return clusterConfigShowCmd.RunE(c, nil) }},
		{"get", func(c *cobra.Command) error { return clusterConfigGetCmd.RunE(c, []string{"k"}) }},
		{"diff", func(c *cobra.Command) error { return clusterConfigDiffCmd.RunE(c, nil) }},
		{"set", func(c *cobra.Command) error { return clusterConfigSetCmd.RunE(c, []string{"k=1"}) }},
		{"reset", func(c *cobra.Command) error { return clusterConfigResetCmd.RunE(c, []string{"k"}) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run(emptyCmd())
			require.Error(t, err, "expected endpoint error for %s", tc.name)
		})
	}
}
