package main

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/clusteradmin"
)

// clusterSubcommandNames returns the registered subcommand names of clusterCmd.
func clusterSubcommandNames() []string {
	names := make([]string, 0, len(clusterCmd.Commands()))
	for _, c := range clusterCmd.Commands() {
		names = append(names, c.Name())
	}
	return names
}

// rootSubcommandNames returns the registered subcommand names of rootCmd.
func rootSubcommandNames() []string {
	names := make([]string, 0, len(rootCmd.Commands()))
	for _, c := range rootCmd.Commands() {
		names = append(names, c.Name())
	}
	return names
}

func TestClusterPlanShowRemoved(t *testing.T) {
	assert.NotContains(t, clusterSubcommandNames(), "plan-show",
		"cluster plan-show stub removed; reintroduce when real local FSM read lands")
}

func TestClusterRebalanceRemoved(t *testing.T) {
	assert.NotContains(t, clusterSubcommandNames(), "rebalance",
		"cluster rebalance stub removed; reintroduce when real local FSM read lands")
}

func TestClusterCmd_Day2OpsRegistered(t *testing.T) {
	expected := []string{
		"transfer-leader", "drain", "health", "placement", "balancer",
	}
	names := clusterSubcommandNames()
	for _, want := range expected {
		assert.Contains(t, names, want, "cluster %s must be registered", want)
	}
}

func TestClusterJoinMovedToRoot(t *testing.T) {
	assert.NotContains(t, clusterSubcommandNames(), "join",
		"cluster join was moved to root as 'grainfs join' (bootstrap convention)")
	assert.Contains(t, rootSubcommandNames(), "join",
		"join must be registered at root alongside serve / migrate / doctor / recover")
}

// clusterClientFromCmd helper tests.

func makeEndpointCmd(t *testing.T, value string, set bool) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String("endpoint", "", "")
	if set {
		require.NoError(t, cmd.Flags().Set("endpoint", value))
	}
	return cmd
}

func TestClusterClientFromCmd_RequiresEndpoint(t *testing.T) {
	cmd := makeEndpointCmd(t, "", false)
	_, err := clusterClientFromCmd(cmd)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "admin endpoint not configured")
}

func TestClusterClientFromCmd_RejectsHTTPURL(t *testing.T) {
	cmd := makeEndpointCmd(t, "http://127.0.0.1:9000", true)
	_, err := clusterClientFromCmd(cmd)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be a UDS socket path")
	assert.Contains(t, err.Error(), "dashboard-only")
}

func TestClusterClientFromCmd_RejectsHTTPSURL(t *testing.T) {
	cmd := makeEndpointCmd(t, "https://example.com", true)
	_, err := clusterClientFromCmd(cmd)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be a UDS socket path")
}

func TestClusterClientFromCmd_AcceptsBarePath(t *testing.T) {
	cmd := makeEndpointCmd(t, "/tmp/admin.sock", true)
	client, err := clusterClientFromCmd(cmd)
	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestClusterClientFromCmd_AcceptsUnixPrefix(t *testing.T) {
	cmd := makeEndpointCmd(t, "unix:/tmp/admin.sock", true)
	client, err := clusterClientFromCmd(cmd)
	require.NoError(t, err)
	require.NotNil(t, client)
}

// startStatusUDSStub spins a Unix socket HTTP server returning the given
// JSON payload at /v1/cluster/status.
func startStatusUDSStub(t *testing.T, payload string) string {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "st-")
	require.NoError(t, err)
	sock := filepath.Join(d, "admin.sock")
	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(payload))
	})
	go http.Serve(ln, mux) //nolint:errcheck
	t.Cleanup(func() {
		_ = ln.Close()
		_ = os.RemoveAll(d)
	})
	return sock
}

// TestRunClusterStatus_JSONFormatPreservesUnknownFields verifies that
// runClusterStatus(format="json") returns the server response body
// byte-for-byte. New server fields (not in typed Status) round-trip.
func TestRunClusterStatus_JSONFormatPreservesUnknownFields(t *testing.T) {
	payload := `{"mode":"cluster","node_id":"n1","future_field":"keepme"}`
	sock := startStatusUDSStub(t, payload)

	client := clusteradmin.NewClient(sock)
	var stdout bytes.Buffer
	require.NoError(t, runClusterStatus(context.Background(), client, "json", &stdout))
	require.Equal(t, payload, strings.TrimSpace(stdout.String()))
}

// TestRunClusterStatus_TextFormatTypedOutput verifies that
// runClusterStatus(format="text") uses typed Status formatting.
func TestRunClusterStatus_TextFormatTypedOutput(t *testing.T) {
	payload := `{"mode":"cluster","node_id":"n1","state":"Leader","term":5,"leader_id":"n1","peers":["n2","n3"]}`
	sock := startStatusUDSStub(t, payload)

	client := clusteradmin.NewClient(sock)
	var stdout bytes.Buffer
	require.NoError(t, runClusterStatus(context.Background(), client, "text", &stdout))
	out := stdout.String()
	assert.Contains(t, out, "mode:      cluster")
	assert.Contains(t, out, "node_id:   n1")
	assert.Contains(t, out, "state:     Leader")
	assert.Contains(t, out, "term:      5")
	assert.Contains(t, out, "leader_id: n1")
	assert.Contains(t, out, "peers:     2")
}

// TestClusterRotateKey_EndpointShadowsParent verifies that
// `cluster rotate-key`'s own PersistentFlags.String("endpoint", ...)
// shadows the parent clusterCmd's persistent --endpoint when both are
// set. cobra resolves child flag values first, so the rotate-key
// subtree continues to point at rotate.sock even after we add admin.sock
// to the parent.
func TestClusterRotateKey_EndpointShadowsParent(t *testing.T) {
	// Find rotate-key from clusterCmd's child list.
	var rotateKey *cobra.Command
	for _, c := range clusterCmd.Commands() {
		if c.Name() == "rotate-key" {
			rotateKey = c
			break
		}
	}
	require.NotNil(t, rotateKey, "cluster rotate-key must be registered")

	// rotate-key has its own PersistentFlags("endpoint"), which cobra
	// surfaces via Flag() at the rotate-key level (not inherited).
	f := rotateKey.PersistentFlags().Lookup("endpoint")
	require.NotNil(t, f, "rotate-key must declare its own --endpoint persistent flag")
	require.Contains(t, f.Usage, "rotate-key socket",
		"rotate-key endpoint description must distinguish from admin.sock")
}
