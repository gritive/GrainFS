package main

import (
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

// startFakeAdminUDS spins a fake admin UDS server and returns the socket path.
// Uses os.MkdirTemp under the OS temp root (not t.TempDir, whose path can exceed
// the platform's sun_path limit on macOS / BSD when the test name is long).
//
// Historical name: this helper used to live in iam_test.go. It's now consumed
// by bucket / bucket_policy / bucket_versioning / bucket_upstream / nfs / cluster
// test files; a follow-up will migrate those to dedicated admin clients and
// drop this shim. Kept here so cmd/grainfs tests still compile after the IAM
// thin-runner refactor.
func startFakeAdminUDS(t *testing.T, mux *http.ServeMux) string {
	t.Helper()
	d, err := os.MkdirTemp("", "iamtest-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	sock := filepath.Join(d, "a.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = ln.Close()
		_ = os.Remove(sock)
	})
	return sock
}
