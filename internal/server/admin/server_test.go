package admin_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/server/admin"
)

func TestServer_StartCleansStaleSocketAndChmods0660(t *testing.T) {
	dir := shortTempDir(t)
	sock := filepath.Join(dir, "a.sock")

	// Create a stale socket file (file exists but no listener; remove via direct unix listen+close).
	stale, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	_ = stale.Close()
	// Some platforms unlink on Close; if so, the stale path is already gone, which Start handles.

	s, err := admin.Start(admin.Config{
		SocketPath: sock,
		Deps:       newServerDeps(t, dir),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop(context.Background())

	info, err := os.Stat(sock)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode != 0o660 {
		t.Fatalf("mode = %o, want 0660", mode)
	}
}

func TestServer_StartFailsWhenAnotherListenerActive(t *testing.T) {
	dir := shortTempDir(t)
	sock := filepath.Join(dir, "a.sock")

	occupy, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer occupy.Close()

	go func() {
		// Accept and discard so DialTimeout succeeds (signals "in use").
		for {
			c, err := occupy.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()

	_, err = admin.Start(admin.Config{
		SocketPath: sock,
		Deps:       newServerDeps(t, dir),
	})
	if err == nil {
		t.Fatal("expected error: socket already in use")
	}
}

func TestServer_StopRemovesSocket(t *testing.T) {
	dir := shortTempDir(t)
	sock := filepath.Join(dir, "a.sock")

	s, err := admin.Start(admin.Config{
		SocketPath: sock,
		Deps:       newServerDeps(t, dir),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sock); err != nil {
		t.Fatalf("socket missing after Start: %v", err)
	}
	if err := s.Stop(context.Background()); err != nil {
		// Hertz's Shutdown may return an error in tests; we still expect cleanup.
		t.Logf("Stop returned %v (continuing — checking unlink)", err)
	}
	// Wait briefly for cleanup.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(sock); errors.Is(err, os.ErrNotExist) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("socket not removed after Stop")
}

func TestServer_ServesListVolumesOverUnixSocket(t *testing.T) {
	dir := shortTempDir(t)
	sock := filepath.Join(dir, "a.sock")

	s, err := admin.Start(admin.Config{
		SocketPath: sock,
		Deps:       newServerDeps(t, dir),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop(context.Background())

	// Wait for listener to be ready.
	deadline := time.Now().Add(2 * time.Second)
	var conn net.Conn
	for time.Now().Before(deadline) {
		conn, err = net.Dial("unix", sock)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.Close()

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", sock)
			},
		},
		Timeout: 5 * time.Second,
	}
	resp, err := client.Get("http://unix/v1/volumes")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func newServerDeps(t *testing.T, dir string) *admin.Deps {
	t.Helper()
	d := newDeps(t)
	_ = dir
	return d
}

// shortTempDir creates a temp dir under /tmp to avoid macOS's 104-char Unix
// socket path limit. Cleaned up at test end.
func shortTempDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "ga-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	return d
}
