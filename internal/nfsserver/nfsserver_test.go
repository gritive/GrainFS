package nfsserver

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestNFSServerStartsAndAcceptsConnections(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)

	srv := NewServer(backend, "nfs-test-vol")

	// Use ephemeral port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	go func() {
		_ = srv.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", port))
	}()

	// Wait for server to be ready
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 500*time.Millisecond)
		if err == nil {
			conn.Close()
			// Server is accepting connections
			t.Cleanup(func() { srv.Close() })
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatal("NFS server did not start accepting connections in time")
}
