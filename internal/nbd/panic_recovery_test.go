//go:build linux

package nbd

import (
	"net"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
	"github.com/stretchr/testify/require"
)

// panicConn is a net.Conn whose Read panics, simulating an unexpected panic
// inside handleConn (e.g. nil pointer dereference in a codec or volume driver).
type panicConn struct {
	net.Conn
}

func (p *panicConn) Read(_ []byte) (int, error) {
	panic("deliberate test panic in NBD connection handler")
}

func (p *panicConn) Write(_ []byte) (int, error) { return 0, nil }
func (p *panicConn) Close() error                { return nil }

// TestHandleConn_PanicRecovery verifies that a panic inside handleConn is
// recovered and does not propagate to the goroutine, which would crash the
// entire NBD server process.
func TestHandleConn_PanicRecovery(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)

	mgr := volume.NewManager(backend)
	_, err = mgr.Create("vol", 1*1024*1024)
	require.NoError(t, err)

	srv := NewServer(mgr, "vol")

	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.handleConn(&panicConn{}) // must not propagate panic
	}()

	select {
	case <-done:
		// handleConn returned — panic was caught by recover()
	case <-time.After(3 * time.Second):
		t.Fatal("handleConn did not return after panic: likely no recover() in place")
	}
}
