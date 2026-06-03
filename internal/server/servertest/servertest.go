// Package servertest exports transport-level test helpers (free port, TCP
// readiness wait, graceful shutdown) so both internal/server tests and the
// satellite packages (e.g. alertssvc) can import them. These were lifted out
// of internal/server/server_test.go, which — being a _test.go file — could not
// be imported by other packages.
package servertest

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/stretchr/testify/require"
)

// TB is the minimal testing surface needed to fail a test from a helper.
type TB interface {
	Helper()
	Errorf(format string, args ...any)
	FailNow()
}

// FatalTB extends TB with Fatalf for helpers that must abort the test.
type FatalTB interface {
	TB
	Fatalf(format string, args ...any)
}

// CleanupTB is the surface a cleanup helper needs: Helper + Logf.
type CleanupTB interface {
	Helper()
	Logf(format string, args ...any)
}

// ShutdownTimeout is the grace period given to a test server's Shutdown.
const ShutdownTimeout = 10 * time.Millisecond

// FreePort returns an OS-assigned free TCP port on the loopback interface.
func FreePort(t TB) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "freePort")
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// WaitTCP blocks until addr accepts a TCP connection or a 2s deadline elapses,
// failing the test (Fatalf) if the server never becomes ready.
func WaitTCP(t FatalTB, addr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 20*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		lastErr = err
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("server %s did not become ready: %v", addr, lastErr)
}

// ShutdownServer gracefully shuts down srv within ShutdownTimeout, logging
// (but not failing) on any error other than a deadline overrun.
func ShutdownServer(t CleanupTB, srv interface{ Shutdown(context.Context) error }) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Logf("test server shutdown: %v", err)
	}
}
