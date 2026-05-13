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
// JSON body on GET /v1/cluster/config and returns the socket path. Mirrors
// startUDSStubServer / startFakeAdminUDS in sibling tests; uses os.MkdirTemp
// under /tmp to keep the path short enough for sun_path on macOS.
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

// recordedPatch captures the last PATCH /v1/cluster/config body and If-Match-Rev
// header against a fake admin UDS for Task 13 CLI set/reset tests.
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

func TestClusterConfigShow_Tabular(t *testing.T) {
	sock := startFakeClusterConfigUDS(t, sampleConfigBody())

	var out bytes.Buffer
	require.NoError(t, runClusterConfigShow(sock, false, &out))
	s := out.String()
	require.Contains(t, s, "REV: 7")
	require.Contains(t, s, "KEY")
	require.Contains(t, s, "EFFECTIVE")
	require.Contains(t, s, "SOURCE")
	require.Contains(t, s, "balancer-imbalance-trigger-pct")
	require.Contains(t, s, "balancer-imbalance-stop-pct")
	require.Contains(t, s, "alert-webhook")
	require.Contains(t, s, "explicit")
	require.Contains(t, s, "default")
}

func TestClusterConfigShow_JSON(t *testing.T) {
	sock := startFakeClusterConfigUDS(t, sampleConfigBody())

	var out bytes.Buffer
	require.NoError(t, runClusterConfigShow(sock, true, &out))

	// --json must emit the raw body verbatim — parseable JSON with rev=7.
	var parsed struct {
		Rev       uint64            `json:"rev"`
		Source    map[string]string `json:"source"`
		Effective map[string]any    `json:"effective"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &parsed))
	require.Equal(t, uint64(7), parsed.Rev)
	require.Equal(t, "explicit", parsed.Source["balancer-imbalance-trigger-pct"])
}

func TestClusterConfigGet_Tabular(t *testing.T) {
	sock := startFakeClusterConfigUDS(t, sampleConfigBody())

	var out bytes.Buffer
	require.NoError(t, runClusterConfigGet(sock, "balancer-imbalance-trigger-pct", false, &out))
	s := out.String()
	require.Contains(t, s, "balancer-imbalance-trigger-pct")
	require.Contains(t, s, "25.5")
	require.Contains(t, s, "explicit")
}

func TestClusterConfigGet_JSON(t *testing.T) {
	sock := startFakeClusterConfigUDS(t, sampleConfigBody())

	var out bytes.Buffer
	require.NoError(t, runClusterConfigGet(sock, "alert-webhook", true, &out))

	var parsed struct {
		Key    string `json:"key"`
		Value  any    `json:"value"`
		Source string `json:"source"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &parsed))
	require.Equal(t, "alert-webhook", parsed.Key)
	require.Equal(t, "https://x.example", parsed.Value)
	require.Equal(t, "explicit", parsed.Source)
}

func TestClusterConfigDiff_OnlyExplicit(t *testing.T) {
	sock := startFakeClusterConfigUDS(t, sampleConfigBody())

	var out bytes.Buffer
	require.NoError(t, runClusterConfigDiff(sock, &out))
	s := out.String()
	// Explicit keys are present:
	require.Contains(t, s, "balancer-imbalance-trigger-pct")
	require.Contains(t, s, "alert-webhook")
	// Default-sourced key is suppressed:
	require.NotContains(t, s, "balancer-imbalance-stop-pct")
}

func TestClusterConfigSet_SingleField(t *testing.T) {
	sock, rec := startFakePatchUDS(t, 8)
	require.NoError(t, runClusterConfigSet(sock, []string{"balancer-imbalance-trigger-pct=27.5"}, 0))
	require.JSONEq(t, `{"balancer-imbalance-trigger-pct":27.5}`, rec.LastPatchBody())
	require.Equal(t, "", rec.LastIfMatchRev())
}

func TestClusterConfigSet_MultiField_Atomic(t *testing.T) {
	sock, rec := startFakePatchUDS(t, 8)
	require.NoError(t, runClusterConfigSet(sock, []string{
		"balancer-imbalance-trigger-pct=30",
		"alert-webhook=https://x.example",
		"balancer-enabled=true",
	}, 0))
	require.JSONEq(t, `{
	  "balancer-imbalance-trigger-pct":30,
	  "alert-webhook":"https://x.example",
	  "balancer-enabled":true
	}`, rec.LastPatchBody())
}

func TestClusterConfigSet_IfMatchRev(t *testing.T) {
	sock, rec := startFakePatchUDS(t, 8)
	require.NoError(t, runClusterConfigSet(sock, []string{"balancer-imbalance-trigger-pct=27"}, 7))
	require.Equal(t, "7", rec.LastIfMatchRev())
}

func TestClusterConfigReset(t *testing.T) {
	sock, rec := startFakePatchUDS(t, 8)
	require.NoError(t, runClusterConfigReset(sock, []string{"balancer-imbalance-trigger-pct", "alert-webhook"}, 0))
	require.JSONEq(t, `{"reset_keys":["balancer-imbalance-trigger-pct","alert-webhook"]}`, rec.LastPatchBody())
}

func TestClusterConfigSet_InvalidKV(t *testing.T) {
	err := runClusterConfigSet("/tmp/unused.sock", []string{"no-equals-sign"}, 0)
	if err == nil || err.Error() == "" {
		t.Fatalf("expected error for invalid kv, got: %v", err)
	}
}

// The following tests exercise the cobra RunE wrappers directly so that the
// error-path and happy-path branches inside the RunE closures are covered.
// Each test constructs a fresh *cobra.Command with the required flags instead
// of going through the real global command tree, keeping tests isolated.

func newClusterTestCmd(sock string, extraFlags ...string) *cobra.Command {
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
	require.Contains(t, cmd.OutOrStdout().(*bytes.Buffer).String(), "REV: 7")
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
	require.Contains(t, cmd.OutOrStdout().(*bytes.Buffer).String(), "alert-webhook")
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

func TestClusterConfigCmds_RunE_EndpointError(t *testing.T) {
	// Verify that each RunE propagates the clusterEndpointFromCmd error when
	// --endpoint is empty. This covers the single error-return statement in each
	// RunE closure that would otherwise be unreachable via direct helper calls.
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
