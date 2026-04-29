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

var nfsVersions = []string{"4.0", "4.1", "4.2"}

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

func withNFSMount(t *testing.T, vers string, fn func(mnt string)) {
	t.Helper()
	slug := strings.ReplaceAll(vers, ".", "")
	name := strings.ReplaceAll(t.Name(), "/", "-")
	mnt := "/mnt/grainfs-colima-" + slug + "-" + name
	runColimaSSH(t, "sudo", "mkdir", "-p", mnt)
	runColimaSSH(t, "sudo", "mount", "-t", "nfs4",
		"-o", fmt.Sprintf("vers=%s,port=%s", vers, colimaNFS4Port),
		fmt.Sprintf("%s:/", colimaHostIP), mnt)
	t.Cleanup(func() {
		colimaSSH("sudo", "umount", "-l", mnt).Run() //nolint:errcheck
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

func testBasicOps(t *testing.T, mnt string) {
	t.Helper()
	testDir := mnt + "/testdir-basicops"
	runColimaSSH(t, "sudo", "mkdir", "-p", testDir)

	filePath := testDir + "/file.bin"
	runColimaSSH(t, "sudo", "dd",
		"if=/dev/urandom", "of="+filePath,
		"bs=1M", "count=1", "conv=fsync")

	sha := runColimaSSH(t, "sha256sum", filePath)
	origHash := strings.Fields(sha)[0]

	lsOut := runColimaSSH(t, "ls", testDir)
	if !strings.Contains(lsOut, "file.bin") {
		t.Fatalf("readdir: 'file.bin' not found in %q", lsOut)
	}

	renamedPath := testDir + "/renamed.bin"
	runColimaSSH(t, "sudo", "mv", filePath, renamedPath)

	lsAfter := runColimaSSH(t, "ls", testDir)
	if !strings.Contains(lsAfter, "renamed.bin") {
		t.Fatalf("after rename: 'renamed.bin' not found in %q", lsAfter)
	}
	if strings.Contains(lsAfter, "file.bin") {
		t.Fatalf("after rename: 'file.bin' still present in %q", lsAfter)
	}

	sha2 := runColimaSSH(t, "sha256sum", renamedPath)
	if origHash != strings.Fields(sha2)[0] {
		t.Fatalf("sha256 mismatch after rename: orig=%s got=%s", origHash, strings.Fields(sha2)[0])
	}

	runColimaSSH(t, "sudo", "chmod", "750", renamedPath)
	if got := runColimaSSH(t, "stat", "--format=%a", renamedPath); got != "750" {
		t.Fatalf("expected mode 750 after chmod, got %q", got)
	}

	runColimaSSH(t, "sudo", "truncate", "-s", "100", renamedPath)
	if got := runColimaSSH(t, "stat", "--format=%s", renamedPath); got != "100" {
		t.Fatalf("expected size 100 after truncate, got %q", got)
	}

	runColimaSSH(t, "sudo", "rm", "-rf", testDir)

	out, err := colimaSSH("ls", testDir).CombinedOutput()
	if err == nil {
		t.Fatalf("testdir should not exist after rm -rf, but got: %s", out)
	}
}

func testSetAttr(t *testing.T, mnt string) {
	t.Helper()
	testDir := mnt + "/testdir-setattr"
	runColimaSSH(t, "sudo", "mkdir", "-p", testDir)

	filePath := testDir + "/attr.bin"
	runColimaSSH(t, "sudo", "dd",
		"if=/dev/urandom", "of="+filePath,
		"bs=4K", "count=1", "conv=fsync")

	for _, mode := range []string{"600", "644", "750"} {
		runColimaSSH(t, "sudo", "chmod", mode, filePath)
		if got := runColimaSSH(t, "stat", "--format=%a", filePath); got != mode {
			t.Fatalf("chmod %s: expected %s, got %q", mode, mode, got)
		}
	}

	runColimaSSH(t, "sudo", "truncate", "-s", "100", filePath)
	if sz := runColimaSSH(t, "stat", "--format=%s", filePath); sz != "100" {
		t.Fatalf("truncate 100: expected size 100, got %q", sz)
	}

	runColimaSSH(t, "sudo", "truncate", "-s", "8192", filePath)
	if sz := runColimaSSH(t, "stat", "--format=%s", filePath); sz != "8192" {
		t.Fatalf("truncate 8192: expected size 8192, got %q", sz)
	}

	runColimaSSH(t, "sudo", "truncate", "-s", "0", filePath)
	if sz := runColimaSSH(t, "stat", "--format=%s", filePath); sz != "0" {
		t.Fatalf("truncate 0: expected size 0, got %q", sz)
	}

	mtimeBefore := runColimaSSH(t, "stat", "--format=%Y", filePath)
	runColimaSSH(t, "sudo", "env", "TZ=UTC", "touch", "-t", "202001010000", filePath)
	mtimeAfter := runColimaSSH(t, "stat", "--format=%Y", filePath)
	if mtimeBefore == mtimeAfter {
		t.Fatalf("mtime unchanged after touch: %q", mtimeAfter)
	}
	if mtimeAfter != "1577836800" {
		t.Fatalf("mtime expected 1577836800 (2020-01-01 UTC), got %q", mtimeAfter)
	}

	runColimaSSH(t, "sudo", "rm", "-rf", testDir)
}

func testCommit(t *testing.T, mnt string, vers string) {
	t.Helper()
	testDir := mnt + "/testdir-commit"
	runColimaSSH(t, "sudo", "mkdir", "-p", testDir)
	filePath := testDir + "/commit.bin"

	runColimaSSH(t, "sudo", "bash", "-c",
		fmt.Sprintf("dd if=/dev/urandom of=%s bs=64K count=1 conv=fsync", filePath))
	sha1 := strings.Fields(runColimaSSH(t, "sha256sum", filePath))[0]

	// remount and re-read — verifies data was durably committed
	runColimaSSH(t, "sudo", "umount", "-l", mnt)
	runColimaSSH(t, "sudo", "mount", "-t", "nfs4",
		"-o", fmt.Sprintf("vers=%s,port=%s", vers, colimaNFS4Port),
		fmt.Sprintf("%s:/", colimaHostIP), mnt)

	sha2 := strings.Fields(runColimaSSH(t, "sha256sum", filePath))[0]
	if sha1 != sha2 {
		t.Fatalf("commit: sha256 mismatch after remount: before=%s after=%s", sha1, sha2)
	}

	runColimaSSH(t, "sudo", "rm", "-rf", testDir)
}

func TestNFS4_BasicOps(t *testing.T) {
	for _, vers := range nfsVersions {
		vers := vers
		t.Run("vers_"+strings.ReplaceAll(vers, ".", "_"), func(t *testing.T) {
			withNFSMount(t, vers, func(mnt string) {
				testBasicOps(t, mnt)
			})
		})
	}
}

func TestNFS4_SetAttr(t *testing.T) {
	for _, vers := range nfsVersions {
		vers := vers
		t.Run("vers_"+strings.ReplaceAll(vers, ".", "_"), func(t *testing.T) {
			withNFSMount(t, vers, func(mnt string) {
				testSetAttr(t, mnt)
			})
		})
	}
}

func TestNFS4_Commit(t *testing.T) {
	for _, vers := range nfsVersions {
		vers := vers
		t.Run("vers_"+strings.ReplaceAll(vers, ".", "_"), func(t *testing.T) {
			withNFSMount(t, vers, func(mnt string) {
				testCommit(t, mnt, vers)
			})
		})
	}
}

func TestNFS4_Seek(t *testing.T) {
	withNFSMount(t, "4.2", func(mnt string) {
		testDir := mnt + "/testdir-seek"
		runColimaSSH(t, "sudo", "mkdir", "-p", testDir)
		filePath := testDir + "/seek.bin"

		runColimaSSH(t, "sudo", "dd", "if=/dev/urandom", "of="+filePath, "bs=8K", "count=1", "conv=fsync")

		result := runColimaSSH(t, "sudo", "python3", "-c",
			fmt.Sprintf("import os; fd=os.open('%s',os.O_RDONLY); print(os.lseek(fd,0,os.SEEK_DATA)); os.close(fd)", filePath))
		if result != "0" {
			t.Fatalf("SEEK_DATA expected offset 0, got %q", result)
		}

		sz := runColimaSSH(t, "stat", "--format=%s", filePath)
		holeOffset := runColimaSSH(t, "sudo", "python3", "-c",
			fmt.Sprintf("import os; fd=os.open('%s',os.O_RDONLY); print(os.lseek(fd,0,os.SEEK_HOLE)); os.close(fd)", filePath))
		if holeOffset != sz {
			t.Fatalf("SEEK_HOLE expected %s (file size), got %q", sz, holeOffset)
		}

		runColimaSSH(t, "sudo", "rm", "-rf", testDir)
	})
}

func TestNFS4_Allocate(t *testing.T) {
	withNFSMount(t, "4.2", func(mnt string) {
		testDir := mnt + "/testdir-allocate"
		runColimaSSH(t, "sudo", "mkdir", "-p", testDir)
		filePath := testDir + "/alloc.bin"

		runColimaSSH(t, "sudo", "fallocate", "-l", "1M", filePath)
		sz := runColimaSSH(t, "stat", "--format=%s", filePath)
		if sz != "1048576" {
			t.Fatalf("fallocate 1MB: expected size 1048576, got %q", sz)
		}

		runColimaSSH(t, "sudo", "rm", "-rf", testDir)
	})
}

func TestNFS4_Deallocate(t *testing.T) {
	withNFSMount(t, "4.2", func(mnt string) {
		testDir := mnt + "/testdir-deallocate"
		runColimaSSH(t, "sudo", "mkdir", "-p", testDir)
		filePath := testDir + "/dealloc.bin"

		runColimaSSH(t, "sudo", "bash", "-c",
			fmt.Sprintf("python3 -c \"open('%s','wb').write(b'A'*4096)\"", filePath))

		runColimaSSH(t, "sudo", "fallocate", "--punch-hole", "-o", "0", "-l", "1024", filePath)

		firstBytes := runColimaSSH(t, "sudo", "python3", "-c",
			fmt.Sprintf("fd=open('%s','rb'); d=fd.read(4); fd.close(); print(list(d))", filePath))
		if firstBytes != "[0, 0, 0, 0]" {
			t.Fatalf("after punch-hole, expected [0,0,0,0] at start, got %q", firstBytes)
		}

		laterByte := runColimaSSH(t, "sudo", "python3", "-c",
			fmt.Sprintf("fd=open('%s','rb'); fd.seek(1024); d=fd.read(1); fd.close(); print(d[0])", filePath))
		if laterByte != "65" {
			t.Fatalf("byte at offset 1024 should be 'A' (65), got %q", laterByte)
		}

		runColimaSSH(t, "sudo", "rm", "-rf", testDir)
	})
}

func TestNFS4_ServerSideCopy(t *testing.T) {
	withNFSMount(t, "4.2", func(mnt string) {
		testDir := mnt + "/testdir-copy"
		runColimaSSH(t, "sudo", "mkdir", "-p", testDir)
		srcPath := testDir + "/src.bin"
		dstPath := testDir + "/dst.bin"

		runColimaSSH(t, "sudo", "dd", "if=/dev/urandom", "of="+srcPath, "bs=1M", "count=1", "conv=fsync")
		srcHash := strings.Fields(runColimaSSH(t, "sha256sum", srcPath))[0]

		runColimaSSH(t, "sudo", "cp", "--reflink=always", srcPath, dstPath)
		dstHash := strings.Fields(runColimaSSH(t, "sha256sum", dstPath))[0]
		if srcHash != dstHash {
			t.Fatalf("server-side copy sha256 mismatch: src=%s dst=%s", srcHash, dstHash)
		}

		runColimaSSH(t, "sudo", "rm", "-rf", testDir)
	})
}
