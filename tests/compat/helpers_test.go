//go:build compat

package compat

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

// getBinary returns the path to the current (HEAD) grainfs binary.
// Reads GRAINFS_BINARY env var (set by Makefile); falls back to "bin/grainfs".
func getBinary() string {
	if b := os.Getenv("GRAINFS_BINARY"); b != "" {
		return b
	}
	return "bin/grainfs"
}

// uniqueFreePorts allocates n unique free TCP ports and returns them.
func uniqueFreePorts(n int) []int {
	ports := make([]int, 0, n)
	listeners := make([]*net.TCPListener, 0, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			panic(fmt.Sprintf("uniqueFreePorts: %v", err))
		}
		listeners = append(listeners, ln.(*net.TCPListener))
		ports = append(ports, ln.Addr().(*net.TCPAddr).Port)
	}
	for _, ln := range listeners {
		ln.Close()
	}
	return ports
}

// waitForPort waits until a TCP port accepts connections.
func waitForPort(port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("port %d not ready after %v", port, timeout)
}

// terminateProcess sends SIGTERM to a process and waits.
func terminateProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	}
}

// makeSharedEncryptionKeyFile creates a temp file with a 32-byte AES key.
func makeSharedEncryptionKeyFile(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp("", "compat-enc-key-*.bin")
	if err != nil {
		t.Fatalf("makeSharedEncryptionKeyFile: %v", err)
	}
	key := make([]byte, 32)
	for i := range key {
		key[i] = 0xAB
	}
	if _, err := f.Write(key); err != nil {
		t.Fatalf("write enc key: %v", err)
	}
	f.Close()
	t.Cleanup(func() { os.Remove(f.Name()) })
	return f.Name()
}
