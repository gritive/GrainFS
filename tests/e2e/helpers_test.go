package e2e

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

var (
	testServerURL     string
	testServerDataDir string
	testAccessKey     string
	testSecretKey     string
	testS3Client      *s3.Client
	freePortCursor    uint32 = initialFreePortCursor()
)

func keepE2EArtifacts() bool {
	return os.Getenv("GRAINFS_E2E_KEEP_ARTIFACTS") == "1"
}

func initialFreePortCursor() uint32 {
	return uint32((time.Now().UnixNano() + int64(os.Getpid()*7919)) % 25000)
}

func TestMain(m *testing.M) {
	binary := os.Getenv("GRAINFS_BINARY")
	if binary == "" {
		binary = "../../bin/grainfs"
	}

	port := freePort()
	dir, err := os.MkdirTemp("", "grainfs-e2e-go-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "mkdtemp: %v\n", err)
		os.Exit(1)
	}
	cleanupDataDir := func() error {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("cleanup data dir %s: %w", dir, err)
		}
		return nil
	}
	testServerDataDir = dir

	args := []string{"serve", "--data", dir, "--port", fmt.Sprintf("%d", port),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
		"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"}

	// GRAINFS_PPROF=1 enables comprehensive pprof profiling.
	// CPU profile is collected concurrently with the test run (25s window).
	// All profiles are saved to /tmp/grainfs-e2e-*.out after tests complete.
	var pprofPort int
	if os.Getenv("GRAINFS_PPROF") == "1" {
		pprofPort = freePort()
		args = append(args, "--pprof-port", fmt.Sprintf("%d", pprofPort))
	}

	cmd := exec.Command(binary, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "start server: %v\n", err)
		if cleanupErr := cleanupDataDir(); cleanupErr != nil {
			fmt.Fprintln(os.Stderr, cleanupErr)
		}
		os.Exit(1)
	}

	testServerURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	if err := waitForPortM(port, 30*time.Second); err != nil {
		fmt.Fprintln(os.Stderr, err)
		terminateProcess(cmd)
		if cleanupErr := cleanupDataDir(); cleanupErr != nil {
			fmt.Fprintln(os.Stderr, cleanupErr)
		}
		os.Exit(1)
	}

	// Bootstrap admin SA via UDS (replaces legacy --access-key/--secret-key
	// flags). Stash the resulting creds in package-level vars so newS3Client
	// and other helpers can sign with them.
	ak, sk, err := bootstrapAdminViaUDSForTestMain(dir, 30*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bootstrap admin SA: %v\n", err)
		terminateProcess(cmd)
		if cleanupErr := cleanupDataDir(); cleanupErr != nil {
			fmt.Fprintln(os.Stderr, cleanupErr)
		}
		os.Exit(1)
	}
	testAccessKey = ak
	testSecretKey = sk

	// Disable auto-snapshot for deterministic e2e behavior. Tests that need
	// the auto-snapshot loop (e.g. auto_snapshot_test.go) PATCH it back to a
	// non-zero interval explicitly via patchSnapshotInterval.
	if err := patchSnapshotIntervalM(dir, "0s"); err != nil {
		fmt.Fprintf(os.Stderr, "disable auto-snapshot: %v\n", err)
		terminateProcess(cmd)
		if cleanupErr := cleanupDataDir(); cleanupErr != nil {
			fmt.Fprintln(os.Stderr, cleanupErr)
		}
		os.Exit(1)
	}

	testS3Client = newS3Client(testServerURL)

	// Verify SigV4 verifier has the new key wired in.
	if err := waitForIAMReady(testS3Client, 30*time.Second); err != nil {
		fmt.Fprintln(os.Stderr, err)
		terminateProcess(cmd)
		if cleanupErr := cleanupDataDir(); cleanupErr != nil {
			fmt.Fprintln(os.Stderr, cleanupErr)
		}
		os.Exit(1)
	}

	// Start CPU profile concurrently so it captures actual test load.
	var cpuProfileDone <-chan struct{}
	if pprofPort > 0 {
		done := make(chan struct{})
		cpuProfileDone = done
		go func() {
			defer close(done)
			url := fmt.Sprintf("http://127.0.0.1:%d/debug/pprof/profile?seconds=25", pprofPort)
			if err := fetchProfile(url, "/tmp/grainfs-e2e-cpu.out"); err != nil {
				fmt.Fprintf(os.Stderr, "cpu profile: %v\n", err)
				return
			}
			fmt.Fprintf(os.Stderr, "pprof saved: /tmp/grainfs-e2e-cpu.out\n")
		}()
	}

	code := m.Run()

	if pprofPort > 0 {
		<-cpuProfileDone // wait for CPU profile to complete before killing server
		dumpE2EProfiles(pprofPort)
	}

	terminateProcess(cmd)
	if err := cleanupDataDir(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}

func TestWaitForPortsParallelErrWithProcessesReturnsWhenProcessExits(t *testing.T) {
	cmd := exec.Command("sh", "-c", "exit 7")
	require.NoError(t, cmd.Start())

	started := time.Now()
	err := waitForPortsParallelErrWithProcesses([]int{freePort()}, []*exec.Cmd{cmd}, 5*time.Second)

	require.Error(t, err)
	require.Contains(t, err.Error(), "process exited")
	require.Less(t, time.Since(started), time.Second)
}

func TestCombinedOutputWithWaitDelayReturnsWhenDescendantKeepsPipeOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sh", "-c", "sleep 5 & wait")
	started := time.Now()
	out, err := combinedOutputWithWaitDelay(cmd)

	require.Error(t, err)
	require.Empty(t, out)
	require.Less(t, time.Since(started), 2*time.Second)
}

func TestShortTempDirKeepsAdminSocketPathShort(t *testing.T) {
	dir := shortTempDir(t)
	require.Less(t, len(filepath.Join(dir, "admin.sock")), 104)
	require.Less(t, len(filepath.Join(dir, "rotate.sock")), 104)
}

func combinedOutputWithWaitDelay(cmd *exec.Cmd) ([]byte, error) {
	cmd.WaitDelay = time.Second
	return cmd.CombinedOutput()
}

// dumpE2EProfiles fetches pprof profiles from the running server and saves them to /tmp.
// Called only when GRAINFS_PPROF=1. CPU profile is collected separately during test run.
// Inspect results with:
//
//	go tool pprof -top /tmp/grainfs-e2e-cpu.out      # CPU hotspots
//	go tool pprof -top /tmp/grainfs-e2e-mutex.out    # lock contention
//	go tool pprof -top /tmp/grainfs-e2e-allocs.out   # allocation hotspots
//	go tool pprof -top /tmp/grainfs-e2e-heap.out     # live heap
func dumpE2EProfiles(pprofPort int) {
	base := fmt.Sprintf("http://127.0.0.1:%d/debug/pprof", pprofPort)
	profiles := []struct {
		url  string
		file string
	}{
		{base + "/mutex", "/tmp/grainfs-e2e-mutex.out"},
		{base + "/allocs", "/tmp/grainfs-e2e-allocs.out"},
		{base + "/heap", "/tmp/grainfs-e2e-heap.out"},
		{base + "/goroutine", "/tmp/grainfs-e2e-goroutine.out"},
	}
	for _, p := range profiles {
		if err := fetchProfile(p.url, p.file); err != nil {
			fmt.Fprintf(os.Stderr, "pprof dump %s: %v\n", p.file, err)
		} else {
			fmt.Fprintf(os.Stderr, "pprof saved: %s\n", p.file)
		}
	}
	fmt.Fprintf(os.Stderr, "\nAnalyse with:\n")
	fmt.Fprintf(os.Stderr, "  go tool pprof -top /tmp/grainfs-e2e-cpu.out    # CPU hotspots\n")
	fmt.Fprintf(os.Stderr, "  go tool pprof -top /tmp/grainfs-e2e-mutex.out  # lock contention\n")
	fmt.Fprintf(os.Stderr, "  go tool pprof -top /tmp/grainfs-e2e-allocs.out # alloc hotspots\n")
}

func fetchProfile(url, dest string) error {
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return os.WriteFile(dest, data, 0o644)
}

func newS3Client(endpoint string) *s3.Client {
	return s3.New(s3.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider(testAccessKey, testSecretKey, ""),
		UsePathStyle: true,
	})
}

// bootstrapAdminViaUDSForTestMain is a TestMain-friendly variant of
// bootstrapAdminViaUDS — no *testing.T because TestMain doesn't have one.
func bootstrapAdminViaUDSForTestMain(dataDir string, timeout time.Duration) (string, string, error) {
	sock := dataDir + "/admin.sock"
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		if time.Now().After(deadline) {
			return "", "", fmt.Errorf("admin socket %s did not appear within %v", sock, timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return tryBootstrapAdminViaUDS(sock)
}

func freePort() int {
	// Avoid the OS ephemeral range for listeners. The cluster e2e tests open
	// many outbound UDP/QUIC sockets; using :0 for a future UDP listener can
	// race with an ephemeral source port after the probe socket is closed.
	for i := 0; i < 25000; i++ {
		port := 20000 + int(atomic.AddUint32(&freePortCursor, 7919)%25000)
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			continue
		}
		udp, err := net.ListenPacket("udp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			_ = udp.Close()
			_ = l.Close()
			return port
		}
		_ = l.Close()
	}

	for i := 0; i < 100; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			panic(fmt.Sprintf("cannot allocate free port: %v", err))
		}
		port := l.Addr().(*net.TCPAddr).Port
		udp, err := net.ListenPacket("udp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			_ = udp.Close()
			_ = l.Close()
			return port
		}
		_ = l.Close()
	}
	panic("cannot allocate port available for both tcp and udp")
}

func uniqueFreePorts(n int) []int {
	ports := make([]int, 0, n)
	seen := make(map[int]struct{}, n)
	for len(ports) < n {
		p := freePort()
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		ports = append(ports, p)
	}
	return ports
}

// waitForWritableEndpoint probes each endpoint until one accepts the supplied
// operation. Each attempt gets its own timeout so a slow or unready node does
// not monopolize the whole leader search window.
func waitForWritableEndpoint(
	parent context.Context,
	endpoints []string,
	timeout, perAttemptTimeout, interval time.Duration,
	probe func(context.Context, string) error,
) (int, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		for i, endpoint := range endpoints {
			attemptCtx, cancel := context.WithTimeout(parent, perAttemptTimeout)
			err := probe(attemptCtx, endpoint)
			cancel()
			if err == nil {
				return i, nil
			}
			lastErr = err
		}
		select {
		case <-parent.Done():
			return -1, parent.Err()
		case <-time.After(interval):
		}
	}
	if lastErr == nil {
		lastErr = errors.New("no writable endpoint found")
	}
	return -1, fmt.Errorf("no writable endpoint found within %s: %w", timeout, lastErr)
}

func waitForPort(t testing.TB, port int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Failf(t, "server did not start", "server did not start on port %d within %v", port, timeout)
}

// waitForPortsParallel waits for all ports to become available concurrently.
// Unlike waitForPort, this does not fail from a goroutine.
func waitForPortsParallel(t testing.TB, ports []int, timeout time.Duration) {
	t.Helper()
	var wg sync.WaitGroup
	failed := make(chan int, len(ports))
	for _, port := range ports {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			deadline := time.Now().Add(timeout)
			for time.Now().Before(deadline) {
				conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", p), 500*time.Millisecond)
				if err == nil {
					conn.Close()
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
			failed <- p
		}(port)
	}
	wg.Wait()
	close(failed)
	for p := range failed {
		t.Errorf("server did not start on port %d within %v", p, timeout)
	}
	if t.Failed() {
		t.FailNow()
	}
}

// waitForPortsParallelErr is a non-fatal variant of waitForPortsParallel.
// Returns the first port that did not come up within timeout, or nil on success.
// Used by retry-aware test helpers (e.g., mrCluster) that need to recover
// from transient bind failures rather than abort the whole test.
func waitForPortsParallelErr(ports []int, timeout time.Duration) error {
	return waitForPortsParallelErrWithProcesses(ports, nil, timeout)
}

func waitForPortsParallelErrWithProcesses(ports []int, procs []*exec.Cmd, timeout time.Duration) error {
	var wg sync.WaitGroup
	failed := make(chan error, len(ports))
	for idx, port := range ports {
		var proc *exec.Cmd
		if idx < len(procs) {
			proc = procs[idx]
		}
		wg.Add(1)
		go func(p int, cmd *exec.Cmd) {
			defer wg.Done()
			deadline := time.Now().Add(timeout)
			for time.Now().Before(deadline) {
				conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", p), 500*time.Millisecond)
				if err == nil {
					conn.Close()
					return
				}
				if exited, detail := processExited(cmd); exited {
					failed <- fmt.Errorf("server process exited before port %d became ready: %s", p, detail)
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
			failed <- fmt.Errorf("server did not start on port %d within %v", p, timeout)
		}(port, proc)
	}
	wg.Wait()
	close(failed)
	for err := range failed {
		return err
	}
	return nil
}

func processExited(cmd *exec.Cmd) (bool, string) {
	if cmd == nil || cmd.Process == nil {
		return false, ""
	}
	var status syscall.WaitStatus
	var usage syscall.Rusage
	pid, err := syscall.Wait4(cmd.Process.Pid, &status, syscall.WNOHANG, &usage)
	if pid == 0 {
		return false, ""
	}
	if err != nil {
		if errors.Is(err, syscall.ECHILD) {
			return true, "already reaped"
		}
		return false, ""
	}
	if status.Exited() {
		return true, fmt.Sprintf("exit status %d", status.ExitStatus())
	}
	if status.Signaled() {
		return true, fmt.Sprintf("signal %s", status.Signal())
	}
	return true, fmt.Sprintf("wait status %d", status)
}

// waitForPortM is the TestMain variant of waitForPort — no *testing.T available there.
func waitForPortM(port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("server did not start on port %d within %v", port, timeout)
}

func waitForS3Write(t testing.TB, client *s3.Client, bucket, key string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader("ready"),
		})
		cancel()
		if err == nil {
			return
		}
		lastErr = err
		time.Sleep(250 * time.Millisecond)
	}
	require.Failf(t, "bucket did not become writable", "bucket %s did not become writable within %v: %v", bucket, timeout, lastErr)
}

func startIsolatedE2EServer(t testing.TB) (string, *s3.Client) {
	t.Helper()
	dir, err := os.MkdirTemp("", "grainfs-e2e-isolated-*")
	require.NoError(t, err, "mkdtemp")
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	port := freePort()
	cmd := exec.Command(getBinary(), "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
		"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start(), "start isolated server")
	t.Cleanup(func() { terminateProcess(cmd) })

	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(t, port, 30*time.Second)

	// Bootstrap an admin SA via UDS for this isolated server. Each call
	// gets its own creds since each call has its own data dir.
	ak, sk := bootstrapAdminViaUDS(t, dir)
	// Disable auto-snapshot for deterministic e2e behavior. Tests that need
	// the auto-snapshot loop PATCH it back to a non-zero interval explicitly.
	patchSnapshotInterval(t, dir, "0s")
	cli := s3ClientFor(url, ak, sk)
	require.NoError(t, waitForIAMReady(cli, 30*time.Second))
	return url, cli
}

// waitForIAMReady polls HeadBucket against a probe bucket name until the IAM
// bootstrap shim has registered the static test/test credentials. The shim
// runs in a goroutine after the data port opens, so the brief gap between
// "port up" and "auth ready" must be absorbed here. Returns nil once any
// auth-ready response (200 or 404 for a non-existent bucket) is observed,
// or the latest error on timeout.
func waitForIAMReady(cli *s3.Client, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := cli.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("__bootstrap_probe__")})
		cancel()
		if err == nil {
			return nil
		}
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "NotFound", "NoSuchBucket":
				return nil
			}
		}
		lastErr = err
		if time.Now().After(deadline) {
			return fmt.Errorf("IAM bootstrap not ready within %v: %w", timeout, lastErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func createBucketWithClient(t testing.TB, client *s3.Client, name string) {
	t.Helper()
	ctx := context.Background()
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(name),
	})
	require.NoError(t, err, "create bucket %s", name)
	const readinessKey = "__grainfs_e2e_ready"
	waitForS3Write(t, client, name, readinessKey, 30*time.Second)
	_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(name),
		Key:    aws.String(readinessKey),
	})

	t.Cleanup(func() {
		out, _ := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(name),
		})
		if out != nil {
			for _, obj := range out.Contents {
				_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(name),
					Key:    obj.Key,
				})
			}
		}
		_, _ = client.DeleteBucket(ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(name),
		})
	})
}

func terminateProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil || cmd.ProcessState != nil {
		return
	}
	_ = cmd.Process.Kill()
	_, _ = cmd.Process.Wait()
}

// createBucket creates a bucket for testing and returns a cleanup function.
func createBucket(t *testing.T, name string) {
	t.Helper()
	ctx := context.Background()
	_, err := testS3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(name),
	})
	require.NoError(t, err, "create bucket %s", name)

	t.Cleanup(func() {
		// delete all objects first
		out, _ := testS3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(name),
		})
		if out != nil {
			for _, obj := range out.Contents {
				testS3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(name),
					Key:    obj.Key,
				})
			}
		}
		testS3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(name),
		})
	})
}
