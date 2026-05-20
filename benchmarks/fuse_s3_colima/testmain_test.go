//go:build colima

package fuse_s3_colima

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"
)

var (
	colimaHostIP   = envOrDefault("HOST_IP", "192.168.5.2")
	colimaHTTPPort = envOrDefault("HTTP_PORT", "19201")
	accessKey      = envOrDefault("S3_ACCESS_KEY", "")
	secretKey      = envOrDefault("S3_SECRET_KEY", "")
	bucket         = envOrDefault("S3_BUCKET", "fuse-colima-test")
	clusterKey     = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
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

func TestMain(m *testing.M) {
	if err := exec.Command("colima", "status").Run(); err != nil {
		fmt.Fprintln(os.Stderr, "[colima] not running; starting colima...")
		out, startErr := exec.Command("colima", "start").CombinedOutput()
		if startErr != nil {
			fmt.Fprintf(os.Stderr, "colima start failed: %v\n%s\n", startErr, out)
			os.Exit(1)
		}
	}

	if os.Getenv("SKIP_BUILD") != "1" {
		fmt.Println("[colima] building grainfs...")
		out, err := exec.Command("make", "-C", "../..", "build").CombinedOutput()
		if err != nil {
			fmt.Fprintf(os.Stderr, "make build failed: %v\n%s\n", err, out)
			os.Exit(1)
		}
	}

	dataDir, err := os.MkdirTemp("", "grainfs-fuse-s3-colima-bench-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "mkdtemp: %v\n", err)
		os.Exit(1)
	}

	binary := os.Getenv("GRAINFS_BINARY")
	if binary == "" {
		binary = "../../bin/grainfs"
	}

	cmd := exec.Command(binary,
		"serve",
		"--data", dataDir,
		"--port", colimaHTTPPort,
		"--nfs4-port", "0",
		"--nbd-port", "0",
		"--cluster-key", clusterKey,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "start grainfs: %v\n", err)
		os.RemoveAll(dataDir)
		os.Exit(1)
	}

	healthURL := fmt.Sprintf("http://127.0.0.1:%s/", colimaHTTPPort)
	deadline := time.Now().Add(15 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		resp, err := http.Get(healthURL) //nolint:noctx
		if err == nil {
			resp.Body.Close()
			ready = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !ready {
		fmt.Fprintln(os.Stderr, "grainfs did not become healthy within 15s")
		cmd.Process.Kill()
		os.RemoveAll(dataDir)
		os.Exit(1)
	}

	if accessKey == "" || secretKey == "" {
		adminSock := dataDir + "/admin.sock"
		out, err := exec.Command(binary, "iam", "--json", "sa", "create", "fuse-colima-bench", "--endpoint", adminSock).CombinedOutput()
		if err != nil {
			fmt.Fprintf(os.Stderr, "bootstrap fuse colima credentials failed: %v\n%s\n", err, out)
			cmd.Process.Kill()
			os.RemoveAll(dataDir)
			os.Exit(1)
		}
		var creds struct {
			AccessKey string `json:"access_key"`
			SecretKey string `json:"secret_key"`
		}
		if err := json.Unmarshal(out, &creds); err != nil {
			fmt.Fprintf(os.Stderr, "parse fuse colima credentials failed: %v\n%s\n", err, out)
			cmd.Process.Kill()
			os.RemoveAll(dataDir)
			os.Exit(1)
		}
		accessKey = creds.AccessKey
		secretKey = creds.SecretKey
	}

	code := m.Run()

	cmd.Process.Kill()
	cmd.Wait() //nolint:errcheck
	os.RemoveAll(dataDir)
	os.Exit(code)
}
