package e2e

import (
	"context"
	"fmt"
	"net"
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

	cmd := exec.Command(binary, "serve", "--data", dir, "--port", fmt.Sprintf("%d", port),
		"--nfs-port", fmt.Sprintf("%d", nfsPort),
		"--nfs4-port", fmt.Sprintf("%d", freePort()))
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
	cmd.Process.Kill()
	os.Exit(code)
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
