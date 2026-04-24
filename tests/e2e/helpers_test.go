package e2e

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	testServerURL string
	testS3Client  *s3.Client
	testNFSPort   int
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

	nfsPort := freePort()

	args := []string{"serve", "--data", dir, "--port", fmt.Sprintf("%d", port),
		"--nfs-port", fmt.Sprintf("%d", nfsPort),
		"--nfs4-port", fmt.Sprintf("%d", freePort())}

	// GRAINFS_PPROF=1 enables pprof profiling. Profiles are saved to /tmp/grainfs-e2e-*.out
	// after all tests complete. Inspect with: go tool pprof -top /tmp/grainfs-e2e-mutex.out
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
	defer cmd.Process.Kill()

	testServerURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	testNFSPort = nfsPort
	waitForPortM(port, 5*time.Second)
	waitForPortM(nfsPort, 5*time.Second)

	testS3Client = newS3Client(testServerURL)

	code := m.Run()

	if pprofPort > 0 {
		dumpE2EProfiles(pprofPort)
	}

	cmd.Process.Kill()
	os.Exit(code)
}

// dumpE2EProfiles fetches pprof profiles from the running server and saves them to /tmp.
// Called only when GRAINFS_PPROF=1. Inspect results with:
//
//	go tool pprof -top /tmp/grainfs-e2e-mutex.out
//	go tool pprof -top /tmp/grainfs-e2e-heap.out
func dumpE2EProfiles(pprofPort int) {
	base := fmt.Sprintf("http://127.0.0.1:%d/debug/pprof", pprofPort)
	profiles := []struct {
		url  string
		file string
	}{
		{base + "/mutex", "/tmp/grainfs-e2e-mutex.out"},
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
	fmt.Fprintf(os.Stderr, "\nAnalyse with:\n  go tool pprof -top /tmp/grainfs-e2e-mutex.out\n")
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
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("cannot allocate free port: %v", err))
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
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
