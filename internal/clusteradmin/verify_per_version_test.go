package clusteradmin

import (
	"bytes"
	"context"
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

// startVerifyUDSStub spins a Unix socket HTTP server returning the given
// PerVersionCutoverReadiness payload at the verify-per-version-cutover endpoint.
func startVerifyUDSStub(t *testing.T, r server.PerVersionCutoverReadiness) string {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "vv-")
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

// TestRunVerifyPerVersion_AllReady asserts that when the server reports
// complete=5, excluded=1, gaps=0, stuck=0, unknown=0, the output contains
// the counts and RunVerifyPerVersion returns nil (exit 0).
func TestRunVerifyPerVersion_AllReady(t *testing.T) {
	sock := startVerifyUDSStub(t, server.PerVersionCutoverReadiness{
		Complete: 5,
		Excluded: 1,
	})

	var out bytes.Buffer
	err := RunVerifyPerVersion(context.Background(), VerifyPerVersionCutoverOptions{
		Endpoint: sock,
		Out:      &out,
	})
	require.NoError(t, err, "all-ready must exit 0")
	s := out.String()
	assert.Contains(t, s, "complete: 5")
	assert.Contains(t, s, "gaps:     0")
	assert.Contains(t, s, "stuck:    0")
	assert.Contains(t, s, "unknown:  0")
	assert.Contains(t, s, "excluded: 1")
	assert.Contains(t, s, "this node only")
}

// TestRunVerifyPerVersion_WithGaps asserts non-zero exit when gaps > 0.
func TestRunVerifyPerVersion_WithGaps(t *testing.T) {
	sock := startVerifyUDSStub(t, server.PerVersionCutoverReadiness{
		Complete: 3,
		Gaps:     2,
		GapRefs:  []string{"b/k@v1", "b/k@v2"},
	})

	var out bytes.Buffer
	err := RunVerifyPerVersion(context.Background(), VerifyPerVersionCutoverOptions{
		Endpoint: sock,
		Out:      &out,
	})
	require.Error(t, err, "non-zero gaps must return error")
	assert.Contains(t, err.Error(), "gaps=2")
	s := out.String()
	assert.Contains(t, s, "gaps:     2")
	assert.Contains(t, s, "b/k@v1")
	assert.Contains(t, s, "b/k@v2")
}

// TestRunVerifyPerVersion_WithStuck asserts non-zero exit when stuck > 0.
func TestRunVerifyPerVersion_WithStuck(t *testing.T) {
	sock := startVerifyUDSStub(t, server.PerVersionCutoverReadiness{
		Stuck:     1,
		StuckRefs: []string{"b/stuck-key@vid"},
	})

	var out bytes.Buffer
	err := RunVerifyPerVersion(context.Background(), VerifyPerVersionCutoverOptions{
		Endpoint: sock,
		Out:      &out,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stuck=1")
	assert.Contains(t, out.String(), "stuck-key")
}

// TestRunVerifyPerVersion_WithUnknown asserts non-zero exit when unknown > 0.
func TestRunVerifyPerVersion_WithUnknown(t *testing.T) {
	sock := startVerifyUDSStub(t, server.PerVersionCutoverReadiness{
		Unknown:     1,
		UnknownRefs: []string{"b/err-key@vid"},
	})

	var out bytes.Buffer
	err := RunVerifyPerVersion(context.Background(), VerifyPerVersionCutoverOptions{
		Endpoint: sock,
		Out:      &out,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown=1")
	assert.Contains(t, out.String(), "err-key")
}

// TestRunVerifyPerVersion_BucketFilter verifies that --bucket is forwarded
// as a query param to the server.
func TestRunVerifyPerVersion_BucketFilter(t *testing.T) {
	d, err := os.MkdirTemp("/tmp", "vvb-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	sock := filepath.Join(d, "admin.sock")
	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)

	var gotBucket string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/verify-per-version-cutover", func(w http.ResponseWriter, req *http.Request) {
		gotBucket = req.URL.Query().Get("bucket")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(server.PerVersionCutoverReadiness{Complete: 1})
	})
	go http.Serve(ln, mux) //nolint:errcheck
	t.Cleanup(func() { _ = ln.Close() })

	var out bytes.Buffer
	err = RunVerifyPerVersion(context.Background(), VerifyPerVersionCutoverOptions{
		Endpoint: sock,
		Bucket:   "mybucket",
		Out:      &out,
	})
	require.NoError(t, err)
	assert.Equal(t, "mybucket", gotBucket)
}
