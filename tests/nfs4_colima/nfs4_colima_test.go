//go:build colima

// Package nfs4_colima contains Colima Linux VM → macOS GrainFS NFSv4 e2e tests.
// Run with: make test-nfs4-colima
// Requires: colima running, nfs-common installed in VM.
package nfs4_colima

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
	colimaNFS4Port = envOrDefault("NFS4_PORT", "19249")
	colimaHTTPPort = envOrDefault("HTTP_PORT", "19200")
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

	dataDir, err := os.MkdirTemp("", "grainfs-colima-e2e-*")
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
		"--nfs4-port", colimaNFS4Port,
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

	// HTTP health 폴링 (최대 10초) — GET / 는 listBuckets(200)로 항상 존재
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

	code := m.Run()

	cmd.Process.Kill()
	cmd.Wait()
	os.RemoveAll(dataDir)
	os.Exit(code)
}

func TestNFS4_BasicOps(t *testing.T) {
	mnt := "/mnt/grainfs-colima-e2e"

	runColimaSSH(t, "sudo", "mkdir", "-p", mnt)
	runColimaSSH(t, "sudo", "mount", "-t", "nfs4",
		"-o", fmt.Sprintf("vers=4.0,port=%s", colimaNFS4Port),
		fmt.Sprintf("%s:/", colimaHostIP), mnt)
	t.Cleanup(func() {
		colimaSSH("sudo", "umount", "-l", mnt).Run() //nolint:errcheck
	})

	testDir := mnt + "/testdir-basicops"

	// mkdir
	runColimaSSH(t, "sudo", "mkdir", "-p", testDir)

	// 1MB 랜덤 파일 쓰기
	filePath := testDir + "/file.bin"
	runColimaSSH(t, "sudo", "dd",
		"if=/dev/urandom",
		"of="+filePath,
		"bs=1M", "count=1",
		"conv=fsync",
	)

	// sha256 원본 기록
	sha := runColimaSSH(t, "sha256sum", filePath)
	origHash := strings.Fields(sha)[0]

	// stat — size 검증
	sizeOut := runColimaSSH(t, "stat", "--format=%s", filePath)
	if sizeOut != "1048576" {
		t.Fatalf("expected size 1048576, got %q", sizeOut)
	}

	// readdir — file.bin 포함 확인
	lsOut := runColimaSSH(t, "ls", testDir)
	if !strings.Contains(lsOut, "file.bin") {
		t.Fatalf("readdir: 'file.bin' not found in %q", lsOut)
	}

	// rename
	renamedPath := testDir + "/renamed.bin"
	runColimaSSH(t, "sudo", "mv", filePath, renamedPath)

	// rename 후 검증
	lsAfter := runColimaSSH(t, "ls", testDir)
	if !strings.Contains(lsAfter, "renamed.bin") {
		t.Fatalf("after rename: 'renamed.bin' not found in %q", lsAfter)
	}
	if strings.Contains(lsAfter, "file.bin") {
		t.Fatalf("after rename: 'file.bin' still present in %q", lsAfter)
	}

	// read+verify sha256
	sha2 := runColimaSSH(t, "sha256sum", renamedPath)
	newHash := strings.Fields(sha2)[0]
	if origHash != newHash {
		t.Fatalf("sha256 mismatch after rename: orig=%s got=%s", origHash, newHash)
	}

	// remove
	runColimaSSH(t, "sudo", "rm", "-rf", testDir)

	// 삭제 후 없음 확인
	out, err := colimaSSH("ls", testDir).CombinedOutput()
	if err == nil {
		t.Fatalf("testdir should not exist after rm -rf, but got: %s", out)
	}
}
