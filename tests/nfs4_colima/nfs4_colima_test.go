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

	"github.com/stretchr/testify/require"
)

var (
	colimaHostIP          = envOrDefault("HOST_IP", "192.168.5.2")
	colimaNFS4Port        = envOrDefault("NFS4_PORT", "19249")
	colimaHTTPPort        = envOrDefault("HTTP_PORT", "19200")
	clusterKey            = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	colimaAdminSock       string
	colimaNFSExportBucket = "colima-nfs-export"
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
	require.NoErrorf(t, err, "colima ssh %v\n%s", args, out)
	return strings.TrimSpace(string(out))
}

func withNFSRootMount(t *testing.T, vers string, fn func(mnt string)) {
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

func withNFSMount(t *testing.T, vers string, fn func(mnt string)) {
	t.Helper()
	withNFSRootMount(t, vers, func(mnt string) {
		fn(mnt + "/" + colimaNFSExportBucket)
	})
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
	colimaAdminSock = dataDir + "/admin.sock"

	args := []string{
		"serve",
		"--data", dataDir,
		"--port", colimaHTTPPort,
		"--nfs4-port", colimaNFS4Port,
		"--nbd-port", "0",
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

	// HTTP health polling: auth may reject GET / before bootstrap, but any
	// HTTP response means the server is accepting requests.
	healthURL := fmt.Sprintf("http://127.0.0.1:%s/", colimaHTTPPort)
	deadline := time.Now().Add(10 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		resp, err := http.Get(healthURL) //nolint:noctx
		if err == nil {
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

	out, err := exec.Command(binary, "bucket", "create", colimaNFSExportBucket, "--endpoint", colimaAdminSock).CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "bucket create failed: %v\n%s\n", err, out)
		cmd.Process.Kill()
		os.RemoveAll(dataDir)
		os.Exit(1)
	}
	out, err = exec.Command(binary, "nfs", "export", "add", colimaNFSExportBucket, "--endpoint", colimaAdminSock).CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "nfs export add failed: %v\n%s\n", err, out)
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
	require.Contains(t, lsOut, "file.bin", "readdir output")

	renamedPath := testDir + "/renamed.bin"
	runColimaSSH(t, "sudo", "mv", filePath, renamedPath)

	lsAfter := runColimaSSH(t, "ls", testDir)
	require.Contains(t, lsAfter, "renamed.bin", "after rename output")
	require.NotContains(t, lsAfter, "file.bin", "after rename output")

	sha2 := runColimaSSH(t, "sha256sum", renamedPath)
	require.Equal(t, origHash, strings.Fields(sha2)[0], "sha256 mismatch after rename")

	runColimaSSH(t, "sudo", "chmod", "750", renamedPath)
	require.Equal(t, "750", runColimaSSH(t, "stat", "--format=%a", renamedPath), "mode after chmod")

	runColimaSSH(t, "sudo", "truncate", "-s", "100", renamedPath)
	require.Equal(t, "100", runColimaSSH(t, "stat", "--format=%s", renamedPath), "size after truncate")

	runColimaSSH(t, "sudo", "rm", "-rf", testDir)

	out, err := colimaSSH("ls", testDir).CombinedOutput()
	require.Errorf(t, err, "testdir should not exist after rm -rf, but got: %s", out)
}

func TestNFS4_MultiExportPseudoRootListsBucket(t *testing.T) {
	withNFSRootMount(t, "4.1", func(mnt string) {
		require.Contains(t, runColimaSSH(t, "ls", mnt), colimaNFSExportBucket)
	})
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
		require.Equalf(t, mode, runColimaSSH(t, "stat", "--format=%a", filePath), "chmod %s", mode)
	}

	runColimaSSH(t, "sudo", "truncate", "-s", "100", filePath)
	require.Equal(t, "100", runColimaSSH(t, "stat", "--format=%s", filePath), "truncate 100")

	runColimaSSH(t, "sudo", "truncate", "-s", "8192", filePath)
	require.Equal(t, "8192", runColimaSSH(t, "stat", "--format=%s", filePath), "truncate 8192")

	runColimaSSH(t, "sudo", "truncate", "-s", "0", filePath)
	require.Equal(t, "0", runColimaSSH(t, "stat", "--format=%s", filePath), "truncate 0")

	mtimeBefore := runColimaSSH(t, "stat", "--format=%Y", filePath)
	runColimaSSH(t, "sudo", "env", "TZ=UTC", "touch", "-t", "202001010000", filePath)
	mtimeAfter := runColimaSSH(t, "stat", "--format=%Y", filePath)
	require.NotEqual(t, mtimeBefore, mtimeAfter, "mtime after touch")
	require.Equal(t, "1577836800", mtimeAfter, "mtime should be 2020-01-01 UTC")

	runColimaSSH(t, "sudo", "rm", "-rf", testDir)
}

func testCommit(t *testing.T, mountPoint string, mnt string, vers string) {
	t.Helper()
	testDir := mnt + "/testdir-commit"
	runColimaSSH(t, "sudo", "mkdir", "-p", testDir)
	filePath := testDir + "/commit.bin"

	runColimaSSH(t, "sudo", "bash", "-c",
		fmt.Sprintf("dd if=/dev/urandom of=%s bs=64K count=1 conv=fsync", filePath))
	sha1 := strings.Fields(runColimaSSH(t, "sha256sum", filePath))[0]

	// remount and re-read — verifies data was durably committed
	runColimaSSH(t, "sudo", "umount", "-l", mountPoint)
	runColimaSSH(t, "sudo", "mount", "-t", "nfs4",
		"-o", fmt.Sprintf("vers=%s,port=%s", vers, colimaNFS4Port),
		fmt.Sprintf("%s:/", colimaHostIP), mountPoint)

	sha2 := strings.Fields(runColimaSSH(t, "sha256sum", filePath))[0]
	require.Equal(t, sha1, sha2, "commit sha256 after remount")

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
			withNFSRootMount(t, vers, func(mnt string) {
				testCommit(t, mnt, mnt+"/"+colimaNFSExportBucket, vers)
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
		require.Equal(t, "0", result, "SEEK_DATA offset")

		sz := runColimaSSH(t, "stat", "--format=%s", filePath)
		holeOffset := runColimaSSH(t, "sudo", "python3", "-c",
			fmt.Sprintf("import os; fd=os.open('%s',os.O_RDONLY); print(os.lseek(fd,0,os.SEEK_HOLE)); os.close(fd)", filePath))
		require.Equal(t, sz, holeOffset, "SEEK_HOLE should return file size")

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
		require.Equal(t, "1048576", sz, "fallocate 1MB size")

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
		require.Equal(t, "[0, 0, 0, 0]", firstBytes, "bytes after punch-hole")

		laterByte := runColimaSSH(t, "sudo", "python3", "-c",
			fmt.Sprintf("fd=open('%s','rb'); fd.seek(1024); d=fd.read(1); fd.close(); print(d[0])", filePath))
		require.Equal(t, "65", laterByte, "byte at offset 1024")

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

		runColimaSSH(t, "sudo", "cp", srcPath, dstPath)
		dstHash := strings.Fields(runColimaSSH(t, "sha256sum", dstPath))[0]
		require.Equal(t, srcHash, dstHash, "server-side copy sha256")

		runColimaSSH(t, "sudo", "rm", "-rf", testDir)
	})
}
