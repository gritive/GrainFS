//go:build colima

// Package p9_colima contains Colima Linux VM → macOS GrainFS 9P e2e tests.
// Run with: make test-9p-colima
// Requires: colima running, 9p kernel module in VM.
package p9_colima

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/stretchr/testify/require"
)

var (
	colimaHostIP    = envOrDefault("HOST_IP", "192.168.5.2")
	colima9PPort    = envOrDefault("P9_PORT", "19259")
	colimaHTTPPort  = envOrDefault("HTTP_PORT", "19200")
	clusterKey      = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	colimaAdminSock string
	colima9PBucket  = "colima-9p-test"
	colimaAccessKey string
	colimaSecretKey string
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

func runColimaSSH(t testing.TB, args ...string) string {
	t.Helper()
	out, err := colimaSSH(args...).CombinedOutput()
	require.NoErrorf(t, err, "colima ssh %v\n%s", args, out)
	return strings.TrimSpace(string(out))
}

func httpObjectURL(key string) string {
	return fmt.Sprintf("http://127.0.0.1:%s/%s/%s", colimaHTTPPort, colima9PBucket, key)
}

func httpPutObject(t *testing.T, key, body string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, httpObjectURL(key), strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "text/plain")
	s3auth.SignRequest(req, colimaAccessKey, colimaSecretKey, "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func httpGetObject(t *testing.T, key string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, httpObjectURL(key), nil)
	require.NoError(t, err)
	s3auth.SignRequest(req, colimaAccessKey, colimaSecretKey, "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

func p9Path(mnt, name string) string {
	return mnt + "/" + name
}

func writeMountedFile(t *testing.T, path, body string) {
	t.Helper()
	runColimaSSH(t, "sudo", "sh", "-c", fmt.Sprintf("printf %%s %s > %s", shellQuote(body), shellQuote(path)))
}

func with9PMount(t *testing.T, fn func(mnt string)) {
	with9PMountAname(t, "/", fn)
}

func with9PBucketMount(t *testing.T, fn func(mnt string)) {
	with9PMountAname(t, "/"+colima9PBucket, fn)
}

func with9PBucketMountB(b *testing.B, fn func(mnt string)) {
	b.Helper()
	name := strings.ReplaceAll(b.Name(), "/", "-")
	mnt := "/mnt/grainfs-9p-" + name
	runColimaSSH(b, "sudo", "mkdir", "-p", mnt)
	runColimaSSH(b, "sudo", "mount", "-t", "9p",
		"-o", fmt.Sprintf("trans=tcp,port=%s,version=9p2000.L,msize=262144,aname=/%s", colima9PPort, colima9PBucket),
		colimaHostIP, mnt)
	b.Cleanup(func() {
		colimaSSH("sudo", "umount", "-l", mnt).Run() //nolint:errcheck
	})
	fn(mnt)
}

func with9PMountAname(t *testing.T, aname string, fn func(mnt string)) {
	t.Helper()
	name := strings.ReplaceAll(t.Name(), "/", "-")
	mnt := "/mnt/grainfs-9p-" + name
	runColimaSSH(t, "sudo", "mkdir", "-p", mnt)
	runColimaSSH(t, "sudo", "mount", "-t", "9p",
		"-o", fmt.Sprintf("trans=tcp,port=%s,version=9p2000.L,msize=262144,aname=%s", colima9PPort, aname),
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
		"--9p-bind", "0.0.0.0",
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

	saArgs := []string{"iam", "--endpoint", colimaAdminSock, "--json", "sa", "create", "colima-9p-e2e"}
	out, err := exec.Command(binary, saArgs...).CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "iam sa create failed: %v\n%s\n", err, out)
		cmd.Process.Kill()
		os.RemoveAll(dataDir)
		os.Exit(1)
	}
	var saResp struct {
		SAID      string `json:"sa_id"`
		AccessKey string `json:"access_key"`
		SecretKey string `json:"secret_key"`
	}
	if err := json.Unmarshal(out, &saResp); err != nil {
		fmt.Fprintf(os.Stderr, "iam sa create parse failed: %v\n%s\n", err, out)
		cmd.Process.Kill()
		os.RemoveAll(dataDir)
		os.Exit(1)
	}
	grantArgs := []string{"iam", "--endpoint", colimaAdminSock, "grant", "put", saResp.SAID, colima9PBucket, "Admin"}
	if out, err := exec.Command(binary, grantArgs...).CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "iam grant put failed: %v\n%s\n", err, out)
		cmd.Process.Kill()
		os.RemoveAll(dataDir)
		os.Exit(1)
	}
	colimaAccessKey = saResp.AccessKey
	colimaSecretKey = saResp.SecretKey

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
	httpPutObject(t, "test-file.txt", "hello 9p")

	with9PMount(t, func(mnt string) {
		out := runColimaSSH(t, "ls", fmt.Sprintf("%s/%s", mnt, colima9PBucket))
		require.Contains(t, out, "test-file.txt", "uploaded object should appear in 9P directory listing")
	})

	with9PBucketMount(t, func(mnt string) {
		out := runColimaSSH(t, "ls", mnt)
		require.Contains(t, out, "test-file.txt", "uploaded object should appear at bucket aname root")
	})
}

func Test9P_Symlink_CreateReadlinkAndFollow(t *testing.T) {
	unique := fmt.Sprintf("symlink-e2e-%d", time.Now().UnixNano())
	targetName := unique + "-target.txt"
	linkName := unique + "-link.txt"

	httpPutObject(t, targetName, "symlink body")

	with9PBucketMount(t, func(mnt string) {
		cmdCreate := fmt.Sprintf("cd %s && ln -s %s %s", shellQuote(mnt), shellQuote(targetName), shellQuote(linkName))
		runColimaSSH(t, "sudo", "sh", "-c", cmdCreate)

		readlink := runColimaSSH(t, "sudo", "sh", "-c", fmt.Sprintf("cd %s && readlink %s", shellQuote(mnt), shellQuote(linkName)))
		require.Equal(t, targetName, readlink)

		fileBody := runColimaSSH(t, "sudo", "sh", "-c", fmt.Sprintf("cd %s && cat %s", shellQuote(mnt), shellQuote(linkName)))
		require.Equal(t, "symlink body", fileBody)

		ls := runColimaSSH(t, "sudo", "sh", "-c", fmt.Sprintf("cd %s && ls -l %s", shellQuote(mnt), shellQuote(linkName)))
		require.Contains(t, ls, linkName+" -> "+targetName)

		runColimaSSH(t, "sudo", "sh", "-c", fmt.Sprintf("cd %s && rm %s", shellQuote(mnt), shellQuote(linkName)))
		_, status := httpGetObject(t, linkName)
		require.NotEqual(t, http.StatusOK, status)
	})
}

func Test9P_WriteReadAndHTTPVisibility(t *testing.T) {
	with9PBucketMount(t, func(mnt string) {
		writeMountedFile(t, p9Path(mnt, "write-read.txt"), "hello from 9p")

		out := runColimaSSH(t, "sudo", "cat", p9Path(mnt, "write-read.txt"))
		require.Equal(t, "hello from 9p", out)

		status, body := httpGetObject(t, "write-read.txt")
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, "hello from 9p", body)
	})
}

func Test9P_OverwriteTruncatesStaleTail(t *testing.T) {
	httpPutObject(t, "overwrite.txt", "longer original content")

	with9PBucketMount(t, func(mnt string) {
		writeMountedFile(t, p9Path(mnt, "overwrite.txt"), "short")

		out := runColimaSSH(t, "sudo", "cat", p9Path(mnt, "overwrite.txt"))
		require.Equal(t, "short", out)

		status, body := httpGetObject(t, "overwrite.txt")
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, "short", body)
	})
}

func Test9P_TruncateUpdatesHTTPObject(t *testing.T) {
	httpPutObject(t, "truncate.txt", "truncate-me")

	with9PBucketMount(t, func(mnt string) {
		runColimaSSH(t, "sudo", "truncate", "-s", "4", p9Path(mnt, "truncate.txt"))

		out := runColimaSSH(t, "sudo", "cat", p9Path(mnt, "truncate.txt"))
		require.Equal(t, "trun", out)

		status, body := httpGetObject(t, "truncate.txt")
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, "trun", body)
	})
}

func Test9P_ChmodTouchStatAndHideMetadata(t *testing.T) {
	with9PBucketMount(t, func(mnt string) {
		writeMountedFile(t, p9Path(mnt, "metadata.txt"), "metadata")
		runColimaSSH(t, "sudo", "chmod", "600", p9Path(mnt, "metadata.txt"))
		runColimaSSH(t, "sudo", "touch", "-m", "-d", "@1704067200", p9Path(mnt, "metadata.txt"))

		stat := runColimaSSH(t, "sudo", "stat", "-c", "%a %Y", p9Path(mnt, "metadata.txt"))
		require.Equal(t, "600 1704067200", stat)

		out := runColimaSSH(t, "ls", "-a", mnt)
		require.NotContains(t, out, "__meta")
	})
}

func Test9P_RenameAndUnlinkVisibleThroughHTTP(t *testing.T) {
	with9PBucketMount(t, func(mnt string) {
		writeMountedFile(t, p9Path(mnt, "rename-src.txt"), "rename body")
		runColimaSSH(t, "sudo", "mv", p9Path(mnt, "rename-src.txt"), p9Path(mnt, "rename-dst.txt"))

		srcStatus, _ := httpGetObject(t, "rename-src.txt")
		require.NotEqual(t, http.StatusOK, srcStatus)
		dstStatus, dstBody := httpGetObject(t, "rename-dst.txt")
		require.Equal(t, http.StatusOK, dstStatus)
		require.Equal(t, "rename body", dstBody)

		runColimaSSH(t, "sudo", "rm", p9Path(mnt, "rename-dst.txt"))
		status, _ := httpGetObject(t, "rename-dst.txt")
		require.NotEqual(t, http.StatusOK, status)
	})
}

func Benchmark9P_Write1MiB(b *testing.B) {
	const bytesPerRun = 1 << 20
	cases := []struct {
		name  string
		bs    string
		count string
	}{
		{"4KiB_chunks", "4k", "256"},
		{"64KiB_chunks", "64k", "16"},
		{"1MiB_chunk", "1M", "1"},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			with9PBucketMountB(b, func(mnt string) {
				b.SetBytes(bytesPerRun)
				b.ResetTimer()
				script := fmt.Sprintf("set -e; for i in $(seq 0 %d); do dd if=/dev/zero of=%s/bench-$i.bin bs=%s count=%s conv=fsync status=none; done",
					b.N-1, shellQuote(mnt), tc.bs, tc.count)
				runColimaSSH(b, "sudo", "sh", "-c", script)
			})
		})
	}
}
