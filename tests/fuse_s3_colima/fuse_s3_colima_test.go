//go:build colima

// Package fuse_s3_colima contains Colima Linux VM → macOS GrainFS FUSE-over-S3 e2e tests.
//
// What this proves:
//   - GrainFS's S3 API is compatible with standard FUSE-over-S3 client tooling
//     (rclone mount). No custom FUSE protocol or server-side mount is required.
//   - Common filesystem operations (read/write/checksum/mkdir/rm/rename) work
//     against a remote GrainFS server from a Linux client.
//
// Topology:
//   - macOS host runs grainfs serve on port HTTP_PORT.
//   - Colima Linux VM runs `rclone mount` against host IP HOST_IP:HTTP_PORT.
//
// Run: make test-fuse-s3-colima
// Requires: colima running, rclone + fuse3 installed in VM.
package fuse_s3_colima

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
	colimaHostIP   = envOrDefault("HOST_IP", "192.168.5.2")
	colimaHTTPPort = envOrDefault("HTTP_PORT", "19200")
	accessKey      = envOrDefault("S3_ACCESS_KEY", "fuse-colima-test-ak")
	secretKey      = envOrDefault("S3_SECRET_KEY", "fuse-colima-test-sk-1234567890")
	bucket         = envOrDefault("S3_BUCKET", "fuse-colima-test")
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

// runColimaShell runs a bash command inside the VM, used when the command
// needs shell features (pipes, redirects, heredocs).
func runColimaShell(t *testing.T, script string) string {
	t.Helper()
	out, err := colimaSSH("bash", "-c", script).CombinedOutput()
	if err != nil {
		t.Fatalf("colima ssh bash %q: %v\n%s", script, err, out)
	}
	return strings.TrimSpace(string(out))
}

// withRcloneMount configures rclone in the Colima VM, mounts the bucket via
// FUSE, runs fn, and ensures the mount is torn down on return.
func withRcloneMount(t *testing.T, fn func(mnt string)) {
	t.Helper()

	// Make sure rclone + fuse are available in the VM.
	out, err := colimaSSH("which", "rclone").CombinedOutput()
	if err != nil || strings.TrimSpace(string(out)) == "" {
		t.Skip("rclone not installed in colima VM (try: colima ssh -- sudo apt install -y rclone fuse3)")
	}
	if _, err := colimaSSH("test", "-e", "/dev/fuse").CombinedOutput(); err != nil {
		t.Skip("/dev/fuse not present in colima VM")
	}

	name := strings.ReplaceAll(t.Name(), "/", "-")
	// Mount under /tmp so the regular VM user owns the mountpoint — no sudo
	// needed for either rclone mount or the workload commands. This also keeps
	// ownership consistent with --allow-other being unnecessary.
	mnt := "/tmp/grainfs-fuse-s3-" + name

	// Write rclone config inside the VM. Plain v4-signed S3 client.
	cfgPath := "/tmp/rclone-fuse-s3-" + name + ".conf"
	cfg := fmt.Sprintf(`[grainfs]
type = s3
provider = Other
access_key_id = %s
secret_access_key = %s
endpoint = http://%s:%s
region = us-east-1
force_path_style = true
`, accessKey, secretKey, colimaHostIP, colimaHTTPPort)
	runColimaShell(t, fmt.Sprintf("cat > %s <<'EOF'\n%sEOF", cfgPath, cfg))

	// Make sure the bucket exists (idempotent — rclone mkdir is a no-op if present).
	mkdirOut, mkdirErr := colimaSSH("rclone", "--config", cfgPath, "mkdir", "grainfs:"+bucket).CombinedOutput()
	if mkdirErr != nil {
		t.Fatalf("rclone mkdir failed: %v\n%s", mkdirErr, mkdirOut)
	}

	// Prepare mountpoint and mount via rclone --daemon (forks into background).
	runColimaSSH(t, "mkdir", "-p", mnt)

	mountOut, mountErr := colimaSSH("rclone", "--config", cfgPath, "mount",
		"grainfs:"+bucket, mnt,
		"--daemon",
		"--vfs-cache-mode", "writes",
		"--dir-cache-time", "1s",
	).CombinedOutput()
	if mountErr != nil {
		t.Fatalf("rclone mount failed: %v\n%s", mountErr, mountOut)
	}

	// Wait for the mount to be ready (mountpoint -q polls /proc/mounts).
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if err := colimaSSH("mountpoint", "-q", mnt).Run(); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err := colimaSSH("mountpoint", "-q", mnt).Run(); err != nil {
		t.Fatalf("rclone mount did not become ready within 15s: %v", err)
	}

	t.Cleanup(func() {
		colimaSSH("fusermount3", "-u", mnt).Run()    //nolint:errcheck
		colimaSSH("fusermount", "-u", mnt).Run()     //nolint:errcheck
		colimaSSH("sudo", "umount", "-l", mnt).Run() //nolint:errcheck
		colimaSSH("rmdir", mnt).Run()                //nolint:errcheck
		colimaSSH("rm", "-f", cfgPath).Run()         //nolint:errcheck
	})

	fn(mnt)
}

func TestMain(m *testing.M) {
	if err := exec.Command("colima", "status").Run(); err != nil {
		fmt.Fprintln(os.Stderr, "colima not running — skipping all colima tests")
		os.Exit(0)
	}

	if os.Getenv("SKIP_BUILD") != "1" {
		fmt.Println("[colima] building grainfs...")
		out, err := exec.Command("make", "-C", "../..", "build").CombinedOutput()
		if err != nil {
			fmt.Fprintf(os.Stderr, "make build failed: %v\n%s\n", err, out)
			os.Exit(1)
		}
	}

	dataDir, err := os.MkdirTemp("", "grainfs-fuse-s3-colima-*")
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
		"--access-key", accessKey,
		"--secret-key", secretKey,
		"--nfs4-port", "0",
		"--nbd-port", "0",
	}

	cmd := exec.Command(binary, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "start grainfs: %v\n", err)
		os.RemoveAll(dataDir)
		os.Exit(1)
	}

	// Wait until the S3 endpoint accepts requests. GET / returns ListBuckets (200).
	healthURL := fmt.Sprintf("http://127.0.0.1:%s/", colimaHTTPPort)
	deadline := time.Now().Add(15 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		resp, err := http.Get(healthURL) //nolint:noctx
		if err == nil {
			resp.Body.Close()
			// Auth is required, so a 403 here also means the server is up.
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusForbidden {
				ready = true
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !ready {
		fmt.Fprintln(os.Stderr, "grainfs did not become healthy within 15s")
		cmd.Process.Kill()
		os.RemoveAll(dataDir)
		os.Exit(1)
	}

	code := m.Run()

	cmd.Process.Kill()
	cmd.Wait() //nolint:errcheck
	os.RemoveAll(dataDir)
	os.Exit(code)
}

// TestFUSE_S3_Smoke covers the minimum FUSE-over-S3 contract: mount, ls, write,
// read back with checksum, unmount.
func TestFUSE_S3_Smoke(t *testing.T) {
	withRcloneMount(t, func(mnt string) {
		// ls on empty bucket succeeds.
		runColimaSSH(t, "ls", mnt)

		// 1 MiB random write + read-back checksum.
		filePath := mnt + "/smoke.bin"
		runColimaShell(t, fmt.Sprintf(
			"dd if=/dev/urandom of=%s bs=1M count=1 conv=fsync 2>/dev/null && sha256sum %s | awk '{print $1}' > /tmp/src.sum",
			filePath, filePath))

		// rclone --vfs-cache-mode writes flushes on close. sha256sum re-reads via FUSE.
		dst := strings.Fields(runColimaSSH(t, "sha256sum", filePath))[0]
		src := runColimaSSH(t, "cat", "/tmp/src.sum")
		if src != dst {
			t.Fatalf("checksum mismatch: src=%s dst=%s", src, dst)
		}

		runColimaSSH(t, "rm", "-f", filePath, "/tmp/src.sum")
	})
}

// TestFUSE_S3_Directories covers mkdir + nested write + rmdir. S3 has no real
// directories — rclone emulates them as object key prefixes.
func TestFUSE_S3_Directories(t *testing.T) {
	withRcloneMount(t, func(mnt string) {
		nested := mnt + "/d1/d2"
		runColimaSSH(t, "mkdir", "-p", nested)

		runColimaShell(t, fmt.Sprintf("echo hello-grainfs > %s/greet.txt", nested))
		got := runColimaSSH(t, "cat", nested+"/greet.txt")
		if got != "hello-grainfs" {
			t.Fatalf("nested file content mismatch: got %q", got)
		}

		runColimaSSH(t, "rm", nested+"/greet.txt")
		// rmdir of S3-prefix dirs is best-effort; ignore failures (some tools leave markers).
		colimaSSH("rmdir", nested).Run()    //nolint:errcheck
		colimaSSH("rmdir", mnt+"/d1").Run() //nolint:errcheck
	})
}

// TestFUSE_S3_Rename verifies rename round-trip. With S3 this is implemented as
// CopyObject + DeleteObject by rclone — content is preserved but the operation
// is NOT atomic. This is a documented limitation; the test only asserts the
// final state matches.
func TestFUSE_S3_Rename(t *testing.T) {
	withRcloneMount(t, func(mnt string) {
		src := mnt + "/rename-src.bin"
		dst := mnt + "/rename-dst.bin"

		runColimaShell(t, fmt.Sprintf(
			"dd if=/dev/urandom of=%s bs=64K count=1 conv=fsync 2>/dev/null", src))
		srcSum := strings.Fields(runColimaSSH(t, "sha256sum", src))[0]

		runColimaSSH(t, "mv", src, dst)

		if err := colimaSSH("test", "-f", src).Run(); err == nil {
			t.Fatalf("source still exists after rename: %s", src)
		}
		dstSum := strings.Fields(runColimaSSH(t, "sha256sum", dst))[0]
		if srcSum != dstSum {
			t.Fatalf("rename: checksum mismatch (src=%s dst=%s)", srcSum, dstSum)
		}

		runColimaSSH(t, "rm", "-f", dst)
	})
}

// TestFUSE_S3_CrossProtocol verifies that an object written via the S3 API
// (PutObject from the host) becomes visible through the FUSE mount in the VM.
// rclone caches directory listings, so we configure --dir-cache-time=1s and
// poll briefly.
func TestFUSE_S3_CrossProtocol(t *testing.T) {
	withRcloneMount(t, func(mnt string) {
		// Write directly via rclone's S3 client from inside the VM (host port 9000
		// reachable via colimaHostIP). Avoids needing aws-cli on the host.
		cfgPath := "/tmp/rclone-cross.conf"
		cfg := fmt.Sprintf(`[grainfs]
type = s3
provider = Other
access_key_id = %s
secret_access_key = %s
endpoint = http://%s:%s
region = us-east-1
force_path_style = true
`, accessKey, secretKey, colimaHostIP, colimaHTTPPort)
		runColimaShell(t, fmt.Sprintf("cat > %s <<'EOF'\n%sEOF", cfgPath, cfg))
		t.Cleanup(func() { colimaSSH("rm", "-f", cfgPath).Run() }) //nolint:errcheck

		// rclone copyto uploads a single object (PutObject under the hood).
		runColimaShell(t, "echo from-s3-api > /tmp/cross.txt")
		out, err := colimaSSH("rclone", "--config", cfgPath, "copyto",
			"/tmp/cross.txt", "grainfs:"+bucket+"/cross-protocol.txt").CombinedOutput()
		if err != nil {
			t.Fatalf("S3 PUT (rclone copyto) failed: %v\n%s", err, out)
		}

		// Wait for FUSE dir cache to refresh.
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if err := colimaSSH("test", "-f", mnt+"/cross-protocol.txt").Run(); err == nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}

		got := runColimaSSH(t, "cat", mnt+"/cross-protocol.txt")
		if got != "from-s3-api" {
			t.Fatalf("cross-protocol read: got %q, want %q", got, "from-s3-api")
		}

		runColimaSSH(t, "rm", "-f", mnt+"/cross-protocol.txt", "/tmp/cross.txt")
	})
}
