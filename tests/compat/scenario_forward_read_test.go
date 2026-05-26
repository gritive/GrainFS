//go:build compat

// Phase A status: all standalone prev→current restart tests in this file
// are skipped. Phase A is green-field (no legacy <dataDir>/kek.key
// support); reactivation requires Phase B legacy-migration tooling.

package compat

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// TestForwardRead writes data with the previous binary and reads it back
// with the current binary, verifying forward-read compatibility.
func TestForwardRead(t *testing.T) {
	t.Skip("Phase A green-field cutover: legacy <dataDir>/kek.key is refused at boot; cross-version restart tests require Phase B migration support")
	prev := prevBinary(t)
	cur := getBinary()

	ports := uniqueFreePorts(4)
	prevHTTP, curHTTP := ports[0], ports[1]
	prevRaft, curRaft := ports[2], ports[3]

	dataDir, err := os.MkdirTemp("/tmp", "compat-fwd-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dataDir) })

	encKeyFile := makeSharedEncryptionKeyFile(t)

	// Phase 1: start with previous binary, write data.
	cmd1 := startGrainfsNode(t, prev, dataDir, prevHTTP, prevRaft, encKeyFile)
	require.NoError(t, waitForPort(prevHTTP, 60*time.Second), "wait for prev node HTTP port")
	time.Sleep(2 * time.Second)

	ak, sk := bootstrapCompatAdmin(t, dataDir, 30*time.Second)

	prevClient := newCompatS3Client(fmt.Sprintf("http://127.0.0.1:%d", prevHTTP), ak, sk)
	bucket := "forward-read"
	_, err = prevClient.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err, "create bucket with prev binary")

	wantBody := "hello compat"
	_, err = prevClient.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj1"),
		Body:   strings.NewReader(wantBody),
	})
	require.NoError(t, err, "put object with prev binary")

	terminateProcess(cmd1)

	// Phase 2: restart on the same data dir with the current binary.
	cmd2 := startGrainfsNode(t, cur, dataDir, curHTTP, curRaft, encKeyFile)
	t.Cleanup(func() { terminateProcess(cmd2) })
	require.NoError(t, waitForPort(curHTTP, 60*time.Second), "wait for cur node HTTP port")
	time.Sleep(2 * time.Second)

	curClient := newCompatS3Client(fmt.Sprintf("http://127.0.0.1:%d", curHTTP), ak, sk)
	res, err := curClient.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj1"),
	})
	require.NoError(t, err, "get object with cur binary")
	defer res.Body.Close()

	got, err := io.ReadAll(res.Body)
	require.NoError(t, err, "read object body")
	require.Equal(t, wantBody, string(got), "object body mismatch after forward upgrade")
}

// startGrainfsNode starts a single grainfs node and returns the running Cmd.
// The caller is responsible for calling terminateProcess when done.
func startGrainfsNode(t *testing.T, binary, dataDir string, httpPort, raftPort int, encKeyFile string) *exec.Cmd {
	t.Helper()
	if _, err := os.Stat(binary); err != nil {
	}

	logFile, err := os.CreateTemp("", "grainfs-compat-node-*.log")
	require.NoError(t, err, "create node log file")
	t.Cleanup(func() {
		_ = logFile.Close()
		if !t.Failed() {
			_ = os.Remove(logFile.Name())
		} else {
			t.Logf("grainfs node log: %s", logFile.Name())
		}
	})

	args := []string{
		"serve",
		"--data", dataDir,
		"--port", fmt.Sprintf("%d", httpPort),
		"--node-id", "n1",
		"--raft-addr", fmt.Sprintf("127.0.0.1:%d", raftPort),
		"--cluster-key", "COMPAT-KEY",
		"--encryption-key-file", encKeyFile,
		"--nfs4-port", "0",
		"--nbd-port", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	}

	cmd := exec.Command(binary, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	require.NoError(t, cmd.Start(), "start grainfs node")
	return cmd
}

// newCompatS3Client builds an S3 client pointed at the given endpoint.
func newCompatS3Client(endpoint, ak, sk string) *s3.Client {
	return s3.New(s3.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider(ak, sk, ""),
		UsePathStyle: true,
	})
}
