//go:build colima

// Package p9_colima contains Colima Linux VM → macOS GrainFS 9P e2e tests.
// Run with: make test-9p-colima
// Requires: colima running, 9p kernel module in VM.
package p9_colima

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	colimaHostIP    = envOrDefault("HOST_IP", "192.168.5.2")
	colima9PPort    = envOrDefault("P9_PORT", "19259")
	colimaHTTPPort  = envOrDefault("HTTP_PORT", "19200")
	clusterKey      = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	colimaAdminSock string
	colima9PBucket  = "colima-9p-test"
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
	require.NoErrorf(t, err, "colima ssh %v\n%s", args, out)
	return strings.TrimSpace(string(out))
}

func with9PMount(t *testing.T, fn func(mnt string)) {
	t.Helper()
	name := strings.ReplaceAll(t.Name(), "/", "-")
	mnt := "/mnt/grainfs-9p-" + name
	runColimaSSH(t, "sudo", "mkdir", "-p", mnt)
	runColimaSSH(t, "sudo", "mount", "-t", "9p",
		"-o", fmt.Sprintf("trans=tcp,port=%s,version=9p2000.L", colima9PPort),
		colimaHostIP, mnt)
	t.Cleanup(func() {
		colimaSSH("sudo", "umount", "-l", mnt).Run() //nolint:errcheck
	})
	fn(mnt)
}

func TestMain(m *testing.M) {
	if err := exec.Command("colima", "status").Run(); err != nil {
		fmt.Fprintln(os.Stderr, "colima not running — skipping all 9p colima tests")
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

	dataDir, err := os.MkdirTemp("", "grainfs-9p-colima-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "mkdtemp: %v\n", err)
		os.Exit(1)
	}

	binary := os.Getenv("GRAINFS_BINARY")
	if binary == "" {
		binary = "../../bin/grainfs"
	}
	colimaAdminSock = dataDir + "/admin.sock"

	args := []string{
		"serve",
		"--data", dataDir,
		"--port", colimaHTTPPort,
		"--nfs4-port", "0",
		"--nbd-port", "0",
		"--9p-port", colima9PPort,
		"--cluster-key", clusterKey,
	}

	cmd := exec.Command(binary, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "start grainfs: %v\n", err)
		os.RemoveAll(dataDir)
		os.Exit(1)
	}

	// HTTP health polling
	healthURL := fmt.Sprintf("http://127.0.0.1:%s/", colimaHTTPPort)
	deadline := time.Now().Add(10 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		resp, err := http.Get(healthURL)
		if err == nil {
			resp.Body.Close()
			ready = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !ready {
		fmt.Fprintln(os.Stderr, "grainfs did not start in time")
		cmd.Process.Kill()
		os.RemoveAll(dataDir)
		os.Exit(1)
	}

	// Load 9p kernel modules in Colima VM
	colimaSSH("sudo", "modprobe", "9p").Run()           //nolint:errcheck
	colimaSSH("sudo", "modprobe", "9pnet").Run()        //nolint:errcheck
	colimaSSH("sudo", "modprobe", "9pnet_virtio").Run() //nolint:errcheck

	// Create bucket via admin CLI
	bucketArgs := []string{"bucket", "create", colima9PBucket, "--endpoint", colimaAdminSock}
	if out, err := exec.Command(binary, bucketArgs...).CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "bucket create failed: %v\n%s\n", err, out)
		cmd.Process.Kill()
		os.RemoveAll(dataDir)
		os.Exit(1)
	}

	exitCode := m.Run()

	cmd.Process.Kill()
	os.RemoveAll(dataDir)
	os.Exit(exitCode)
}

func Test9P_ListBuckets(t *testing.T) {
	with9PMount(t, func(mnt string) {
		out := runColimaSSH(t, "ls", mnt)
		require.Contains(t, out, colima9PBucket)
	})
}

func Test9P_ListObjects(t *testing.T) {
	// Upload a test object via HTTP PUT using grainfs S3-compatible API.
	uploadURL := fmt.Sprintf("http://127.0.0.1:%s/%s/test-file.txt", colimaHTTPPort, colima9PBucket)
	req, _ := http.NewRequest(http.MethodPut, uploadURL, strings.NewReader("hello 9p"))
	req.Header.Set("Content-Type", "text/plain")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	with9PMount(t, func(mnt string) {
		out := runColimaSSH(t, "ls", fmt.Sprintf("%s/%s", mnt, colima9PBucket))
		require.Contains(t, out, "test-file.txt", "uploaded object should appear in 9P directory listing")
	})
}

// Test9P_ReadObject: S3 PUT requires IAM service account setup.
// Deferred to next spike — object reads are verified in unit tests (TestObjectFile_ReadAt_*).
// Add here when grainfs gets a public-rw bucket ACL CLI or object-put admin command.
