//go:build linux || darwin

package admin

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// shortSockDir creates a temp dir under /tmp to avoid macOS's 104-char Unix
// socket path limit. (server_test.go has the same helper, but it lives in
// package admin_test; this test file is in package admin to reach unexported
// types like peerCredAddr.)
func shortSockDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "pc-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	return d
}

func TestPeerCredListener_ResolvesCallerUID(t *testing.T) {
	dir := shortSockDir(t)
	sockPath := filepath.Join(dir, "test.sock")

	raw, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })

	pcl := newPeerCredListener(raw)

	dialErr := make(chan error, 1)
	go func() {
		c, derr := net.Dial("unix", sockPath)
		if derr != nil {
			dialErr <- derr
			return
		}
		_ = c.Close()
		dialErr <- nil
	}()

	conn, err := pcl.Accept()
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	addr, ok := conn.RemoteAddr().(*peerCredAddr)
	require.True(t, ok, "RemoteAddr() must be *peerCredAddr, got %T", conn.RemoteAddr())
	cred := addr.Cred()
	require.True(t, cred.Resolved, "expected Resolved=true on linux/darwin")
	require.Equal(t, uint32(os.Getuid()), cred.UID)

	require.NoError(t, <-dialErr)
}

func TestPeerCredListener_ClosePropagates(t *testing.T) {
	dir := shortSockDir(t)
	sockPath := filepath.Join(dir, "test.sock")
	raw, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	pcl := newPeerCredListener(raw)
	require.NoError(t, pcl.Close())
	// Close is NOT idempotent — second close errors with net.ErrClosed.
	require.ErrorIs(t, pcl.Close(), net.ErrClosed)
}
