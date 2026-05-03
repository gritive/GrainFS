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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	testServerURL  string
	testS3Client   *s3.Client
	freePortCursor uint32 = uint32(time.Now().UnixNano() % 20000)
)

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
	defer os.RemoveAll(dir)

	args := []string{"serve", "--data", dir, "--port", fmt.Sprintf("%d", port),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--snapshot-interval", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0"}

	// GRAINFS_DEDUP=1 opts into dedup. Default is OFF for the e2e suite because
	// dedup + snapshots is not implemented in Phase A — CoW E2E tests would
	// fail starting from the second snapshot otherwise.
	if os.Getenv("GRAINFS_DEDUP") != "1" {
		args = append(args, "--dedup=false")
	}

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
		os.Exit(1)
	}
	defer terminateProcess(cmd)

	testServerURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPortM(port, 30*time.Second)

	testS3Client = newS3Client(testServerURL)

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
	os.Exit(code)
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
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		UsePathStyle: true,
	})
}

func freePort() int {
	// Avoid the OS ephemeral range for listeners. The cluster e2e tests open
	// many outbound UDP/QUIC sockets; using :0 for a future UDP listener can
	// race with an ephemeral source port after the probe socket is closed.
	for i := 0; i < 25000; i++ {
		port := 20000 + int(atomic.AddUint32(&freePortCursor, 1)%25000)
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
	t.Fatalf("server did not start on port %d within %v", port, timeout)
}

// waitForPortsParallel waits for all ports to become available concurrently.
// Unlike waitForPort, this does not call t.Fatalf from a goroutine.
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
		return fmt.Errorf("server did not start on port %d within %v", p, timeout)
	}
	return nil
}

// waitForPortM is the TestMain variant of waitForPort — no *testing.T available there.
func waitForPortM(port int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Fprintf(os.Stderr, "server did not start on port %d within %v\n", port, timeout)
	os.Exit(1)
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
	t.Fatalf("bucket %s did not become writable within %v: %v", bucket, timeout, lastErr)
}

func startIsolatedE2EServer(t testing.TB) (string, *s3.Client) {
	t.Helper()
	dir, err := os.MkdirTemp("", "grainfs-e2e-isolated-*")
	if err != nil {
		t.Fatalf("mkdtemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	port := freePort()
	cmd := exec.Command(getBinary(), "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--snapshot-interval", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
		"--dedup=false",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start isolated server: %v", err)
	}
	t.Cleanup(func() { terminateProcess(cmd) })

	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(t, port, 30*time.Second)
	return url, newS3Client(url)
}

func createBucketWithClient(t testing.TB, client *s3.Client, name string) {
	t.Helper()
	ctx := context.Background()
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(name),
	})
	if err != nil {
		t.Fatalf("create bucket %s: %v", name, err)
	}
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
	if err != nil {
		t.Fatalf("create bucket %s: %v", name, err)
	}

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
