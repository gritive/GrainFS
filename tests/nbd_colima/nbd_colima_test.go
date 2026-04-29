//go:build colima

// Package nbd_colima contains Colima Linux VM → macOS GrainFS NBD e2e tests.
// Run with: make test-nbd-colima
// Requires: colima running, nbd-client installed in VM.
package nbd_colima

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

var (
	colimaHostIP  = envOrDefault("HOST_IP", "192.168.5.2")
	colimaNBDPort = envOrDefault("NBD_PORT", "19810")
	colimaHTTPPort = envOrDefault("HTTP_PORT", "19200")
	nbdVolSize    = envOrDefault("NBD_VOL_SIZE", fmt.Sprintf("%d", 64*1024*1024)) // 64MB
	nbdDev        = envOrDefault("NBD_DEV", "/dev/nbd0")
)

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func colimaSSH(args ...string) *exec.Cmd {
	return exec.Command("colima", append([]string{"ssh", "--"}, args...)...)
}

func runColimaSSH(t *testing.T, args ...string) string {
	t.Helper()
	out, err := colimaSSH(args...).CombinedOutput()
	if err != nil {
		t.Fatalf("colima ssh %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func runColimaSSHNoFail(args ...string) (string, error) {
	out, err := colimaSSH(args...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

// withNBDDevice connects /dev/nbd0 to GrainFS and runs fn. Disconnects on cleanup.
func withNBDDevice(t *testing.T, fn func(dev string)) {
	t.Helper()

	// Load NBD kernel module.
	colimaSSH("sudo", "modprobe", "nbd", "max_part=0").Run() //nolint:errcheck

	// Disconnect any stale connection first.
	colimaSSH("sudo", "nbd-client", "-d", nbdDev).Run() //nolint:errcheck

	// Connect.
	runColimaSSH(t, "sudo", "nbd-client",
		colimaHostIP, colimaNBDPort, nbdDev,
		"-b", "4096", "-N", "default",
	)
	t.Logf("nbd-client connected: %s → %s:%s", nbdDev, colimaHostIP, colimaNBDPort)

	t.Cleanup(func() {
		colimaSSH("sudo", "nbd-client", "-d", nbdDev).Run() //nolint:errcheck
	})

	fn(nbdDev)
}

func TestMain(m *testing.M) {
	if err := exec.Command("colima", "status").Run(); err != nil {
		fmt.Fprintln(os.Stderr, "colima not running — skipping all nbd colima tests")
		os.Exit(0)
	}

	if os.Getenv("SKIP_BUILD") != "1" {
		fmt.Println("[colima-nbd] building grainfs...")
		out, err := exec.Command("make", "-C", "../..", "build").CombinedOutput()
		if err != nil {
			fmt.Fprintf(os.Stderr, "make build failed: %v\n%s\n", err, out)
			os.Exit(1)
		}
	}

	dataDir, err := os.MkdirTemp("", "grainfs-nbd-colima-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "mkdtemp: %v\n", err)
		os.Exit(1)
	}

	binary := os.Getenv("GRAINFS_BINARY")
	if binary == "" {
		binary = "../../bin/grainfs"
	}

	args := []string{
		"serve",
		"--data", dataDir,
		"--port", colimaHTTPPort,
		"--nbd-port", colimaNBDPort,
		"--nbd-volume-size", nbdVolSize,
		"--nfs4-port", "0",
	}

	cmd := exec.Command(binary, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "start grainfs: %v\n", err)
		os.RemoveAll(dataDir)
		os.Exit(1)
	}

	healthURL := fmt.Sprintf("http://127.0.0.1:%s/", colimaHTTPPort)
	deadline := time.Now().Add(10 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		resp, err := http.Get(healthURL) //nolint:noctx
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			ready = true
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !ready {
		fmt.Fprintln(os.Stderr, "grainfs did not become healthy within 10s")
		cmd.Process.Kill()
		os.RemoveAll(dataDir)
		os.Exit(1)
	}
	fmt.Printf("[colima-nbd] grainfs ready (PID=%d, NBD port=%s)\n", cmd.Process.Pid, colimaNBDPort)

	code := m.Run()

	cmd.Process.Kill()
	cmd.Wait()
	os.RemoveAll(dataDir)
	os.Exit(code)
}

// TestNBD_PatternWriteRead writes two distinct 4KB patterns and reads them back.
func TestNBD_PatternWriteRead(t *testing.T) {
	withNBDDevice(t, func(dev string) {
		// Write pattern 0xAA to block 0.
		runColimaSSH(t, "sudo", "bash", "-c",
			fmt.Sprintf("python3 -c \"import sys; sys.stdout.buffer.write(b'\\xaa'*4096)\" | dd of=%s bs=4096 count=1 2>/dev/null", dev))

		// Write pattern 0xBB to block 1.
		runColimaSSH(t, "sudo", "bash", "-c",
			fmt.Sprintf("python3 -c \"import sys; sys.stdout.buffer.write(b'\\xbb'*4096)\" | dd of=%s bs=4096 seek=1 count=1 2>/dev/null", dev))

		// Verify block 0.
		runColimaSSH(t, "sudo", "python3", "-c",
			fmt.Sprintf("d=open('%s','rb').read(4096); assert d==b'\\xaa'*4096, f'block0 mismatch: {set(d)}'", dev))

		// Verify block 1.
		runColimaSSH(t, "sudo", "bash", "-c",
			fmt.Sprintf("python3 -c \"fd=open('%s','rb'); fd.seek(4096); d=fd.read(4096); assert d==b'\\xbb'*4096, f'block1 mismatch: {set(d)}'\"", dev))

		t.Log("PatternWriteRead: PASS")
	})
}

// TestNBD_LargeBlock writes 1MB of random data and verifies checksum after read-back.
func TestNBD_LargeBlock(t *testing.T) {
	withNBDDevice(t, func(dev string) {
		tmpSrc := "/tmp/nbd_colima_src.bin"
		tmpDst := "/tmp/nbd_colima_dst.bin"

		runColimaSSH(t, "sudo", "dd", "if=/dev/urandom", "of="+tmpSrc, "bs=1M", "count=1")

		sha1 := strings.Fields(runColimaSSH(t, "sha256sum", tmpSrc))[0]

		// Write to NBD device.
		runColimaSSH(t, "sudo", "dd", "if="+tmpSrc, "of="+dev, "bs=1M", "count=1", "conv=fsync")

		// Read back.
		runColimaSSH(t, "sudo", "dd", "if="+dev, "of="+tmpDst, "bs=1M", "count=1")

		sha2 := strings.Fields(runColimaSSH(t, "sha256sum", tmpDst))[0]
		if sha1 != sha2 {
			t.Fatalf("sha256 mismatch: written=%s read=%s", sha1, sha2)
		}

		runColimaSSH(t, "rm", "-f", tmpSrc, tmpDst)
		t.Logf("LargeBlock: PASS sha=%s", sha1)
	})
}

// TestNBD_UnalignedOffset writes and reads data at a non-block-aligned offset.
func TestNBD_UnalignedOffset(t *testing.T) {
	withNBDDevice(t, func(dev string) {
		// Write 32 bytes at offset 100.
		runColimaSSH(t, "sudo", "python3", "-c",
			fmt.Sprintf("fd=open('%s','r+b'); fd.seek(100); fd.write(b'GRAINFS_NBD_TEST_1234567890123'); fd.close()", dev))

		// Read back 32 bytes from offset 100.
		got := runColimaSSH(t, "sudo", "python3", "-c",
			fmt.Sprintf("fd=open('%s','rb'); fd.seek(100); print(fd.read(31))", dev))

		if !strings.Contains(got, "GRAINFS_NBD_TEST") {
			t.Fatalf("unaligned read mismatch: %q", got)
		}
		t.Log("UnalignedOffset: PASS")
	})
}

// TestNBD_MultipleConnections tests two sequential nbd-client connections to the same volume.
func TestNBD_MultipleConnections(t *testing.T) {
	withNBDDevice(t, func(dev string) {
		// Write pattern in first connection.
		runColimaSSH(t, "sudo", "bash", "-c",
			fmt.Sprintf("python3 -c \"import sys; sys.stdout.buffer.write(b'\\xcc'*4096)\" | dd of=%s bs=4096 count=1 conv=fsync 2>/dev/null", dev))
	})

	// Second connection — data should persist (volume backed by object storage).
	withNBDDevice(t, func(dev string) {
		runColimaSSH(t, "sudo", "python3", "-c",
			fmt.Sprintf("d=open('%s','rb').read(4096); assert d==b'\\xcc'*4096, f'persistence mismatch: {set(d)}'", dev))
		t.Log("MultipleConnections: PASS")
	})
}
