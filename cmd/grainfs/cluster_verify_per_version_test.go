package main

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/server"
)

// startVerifyPerVersionStub spins a Unix socket HTTP server returning the
// given readiness payload at the verify-per-version-cutover endpoint.
func startVerifyPerVersionStub(t *testing.T, r server.PerVersionCutoverReadiness) string {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "vv-cmd-")
	require.NoError(t, err)
	sock := filepath.Join(d, "admin.sock")
	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/verify-per-version-cutover", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(r)
	})
	go http.Serve(ln, mux) //nolint:errcheck
	t.Cleanup(func() {
		_ = ln.Close()
		_ = os.RemoveAll(d)
	})
	return sock
}

// TestClusterVerifyPerVersionCmd_Registered verifies the command is wired
// under clusterCmd as "verify-per-version".
func TestClusterVerifyPerVersionCmd_Registered(t *testing.T) {
	names := clusterSubcommandNames()
	assert.Contains(t, names, "verify-per-version",
		"cluster verify-per-version must be registered as a subcommand")
}

// TestClusterVerifyPerVersionCmd_Ready asserts exit 0 and output when
// the server reports all-ready.
func TestClusterVerifyPerVersionCmd_Ready(t *testing.T) {
	sock := startVerifyPerVersionStub(t, server.PerVersionCutoverReadiness{
		Complete: 10,
		Excluded: 2,
	})

	cmd := clusterVerifyPerVersionCmd()
	cmd.Flags().String("endpoint", "", "")
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(out)
	cmd.SetArgs([]string{"--endpoint", sock})
	require.NoError(t, cmd.Execute())
	s := out.String()
	assert.Contains(t, s, "complete: 10")
	assert.Contains(t, s, "excluded: 2")
	assert.Contains(t, s, "this node only")
}

// TestClusterVerifyPerVersionCmd_Gaps asserts non-zero exit when gaps > 0.
func TestClusterVerifyPerVersionCmd_Gaps(t *testing.T) {
	sock := startVerifyPerVersionStub(t, server.PerVersionCutoverReadiness{
		Gaps:    3,
		GapRefs: []string{"b/k1@v1", "b/k2@v2"},
	})

	cmd := clusterVerifyPerVersionCmd()
	cmd.Flags().String("endpoint", "", "")
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(out)
	cmd.SetArgs([]string{"--endpoint", sock})
	err := cmd.Execute()
	require.Error(t, err, "gaps > 0 must yield non-zero exit")
	assert.Contains(t, err.Error(), "gaps=3")
	assert.Contains(t, out.String(), "b/k1@v1")
}

// TestClusterVerifyPerVersionCmd_NameNotCompleteCutover confirms the command
// name is "verify-per-version", not "complete-cutover" (which is the unrelated
// Zero-CA TLS cutover command).
func TestClusterVerifyPerVersionCmd_NameNotCompleteCutover(t *testing.T) {
	cmd := clusterVerifyPerVersionCmd()
	assert.Equal(t, "verify-per-version", cmd.Name())
	assert.NotEqual(t, "complete-cutover", cmd.Name())
}
