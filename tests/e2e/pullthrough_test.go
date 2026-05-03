package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPullThrough_FetchesFromUpstream verifies that a GrainFS instance configured
// with --upstream pulls objects from the upstream S3 source on cache miss and
// serves them as if they were local.
func TestPullThrough_FetchesFromUpstream(t *testing.T) {
	binary := getBinary()
	ctx := context.Background()

	// --- Start upstream GrainFS (acts as MinIO/S3 source) ---
	upDir, err := os.MkdirTemp("", "grainfs-pt-upstream-*")
	require.NoError(t, err)
	defer os.RemoveAll(upDir)

	upPort := freePort()
	upCmd := exec.Command(binary, "serve",
		"--data", upDir,
		"--port", fmt.Sprintf("%d", upPort),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--snapshot-interval", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	)
	upCmd.Stdout = os.Stdout
	upCmd.Stderr = os.Stderr
	require.NoError(t, upCmd.Start())
	defer terminateProcess(upCmd)

	waitForPort(t, upPort, 30*time.Second)
	upEndpoint := fmt.Sprintf("http://127.0.0.1:%d", upPort)
	upClient := newS3Client(upEndpoint)

	// Put an object in the upstream
	_, err = upClient.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("shared")})
	require.NoError(t, err)
	waitForS3Write(t, upClient, "shared", "__grainfs_e2e_ready", 30*time.Second)
	_, err = upClient.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("shared"),
		Key:    aws.String("data/file.txt"),
		Body:   strings.NewReader("upstream-content"),
	})
	require.NoError(t, err)

	// --- Start local GrainFS with --upstream pointing to upEndpoint ---
	localDir, err := os.MkdirTemp("", "grainfs-pt-local-*")
	require.NoError(t, err)
	defer os.RemoveAll(localDir)

	localPort := freePort()
	localCmd := exec.Command(binary, "serve",
		"--data", localDir,
		"--port", fmt.Sprintf("%d", localPort),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--upstream", upEndpoint,
		"--snapshot-interval", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	)
	localCmd.Stdout = os.Stdout
	localCmd.Stderr = os.Stderr
	require.NoError(t, localCmd.Start())
	defer terminateProcess(localCmd)

	waitForPort(t, localPort, 30*time.Second)
	localClient := newS3Client(fmt.Sprintf("http://127.0.0.1:%d", localPort))

	// Bucket must exist on local (pull-through creates it if needed)
	_, err = localClient.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("shared")})
	require.NoError(t, err)
	waitForS3Write(t, localClient, "shared", "__grainfs_e2e_ready", 30*time.Second)

	// GET from local — should pull from upstream (cache miss)
	getResp, err := localClient.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("shared"),
		Key:    aws.String("data/file.txt"),
	})
	require.NoError(t, err, "pull-through GET must succeed")
	defer getResp.Body.Close()

	body, _ := io.ReadAll(getResp.Body)
	assert.Equal(t, "upstream-content", string(body), "pull-through must return upstream content")

	// Second GET — should now be served from local cache (no upstream needed)
	getResp2, err := localClient.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("shared"),
		Key:    aws.String("data/file.txt"),
	})
	require.NoError(t, err, "cached GET must succeed")
	defer getResp2.Body.Close()
	body2, _ := io.ReadAll(getResp2.Body)
	assert.Equal(t, "upstream-content", string(body2))
}

// TestPullthrough_LargeObjectE2E verifies that the 2-pass streaming pull-through
// correctly returns bytes-identical content for a large object (~5 MB).
// This is a regression test for the A1 fix: io.ReadAll OOM → 2-pass streaming.
func TestPullthrough_LargeObjectE2E(t *testing.T) {
	binary := getBinary()
	ctx := context.Background()

	// --- Upstream GrainFS ---
	upDir, err := os.MkdirTemp("", "grainfs-pt-large-up-*")
	require.NoError(t, err)
	defer os.RemoveAll(upDir)

	upPort := freePort()
	upCmd := exec.Command(binary, "serve",
		"--data", upDir,
		"--port", fmt.Sprintf("%d", upPort),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--snapshot-interval", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	)
	upCmd.Stdout = os.Stdout
	upCmd.Stderr = os.Stderr
	require.NoError(t, upCmd.Start())
	defer terminateProcess(upCmd)

	waitForPort(t, upPort, 30*time.Second)
	upEndpoint := fmt.Sprintf("http://127.0.0.1:%d", upPort)
	upClient := newS3Client(upEndpoint)

	_, err = upClient.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("large")})
	require.NoError(t, err)
	waitForS3Write(t, upClient, "large", "__grainfs_e2e_ready", 30*time.Second)

	// 5 MB random payload exercises the 2-pass streaming path.
	payload := make([]byte, 5*1024*1024)
	_, err = rand.Read(payload)
	require.NoError(t, err)

	_, err = upClient.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String("large"),
		Key:           aws.String("bigfile.bin"),
		Body:          bytes.NewReader(payload),
		ContentLength: aws.Int64(int64(len(payload))),
	})
	require.NoError(t, err)

	// --- Local GrainFS with --upstream ---
	localDir, err := os.MkdirTemp("", "grainfs-pt-large-local-*")
	require.NoError(t, err)
	defer os.RemoveAll(localDir)

	localPort := freePort()
	localCmd := exec.Command(binary, "serve",
		"--data", localDir,
		"--port", fmt.Sprintf("%d", localPort),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--upstream", upEndpoint,
		"--snapshot-interval", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	)
	localCmd.Stdout = os.Stdout
	localCmd.Stderr = os.Stderr
	require.NoError(t, localCmd.Start())
	defer terminateProcess(localCmd)

	waitForPort(t, localPort, 30*time.Second)
	localClient := newS3Client(fmt.Sprintf("http://127.0.0.1:%d", localPort))

	_, err = localClient.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("large")})
	require.NoError(t, err)
	waitForS3Write(t, localClient, "large", "__grainfs_e2e_ready", 30*time.Second)

	// First GET: cache miss → pull-through streaming fetch.
	getResp, err := localClient.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("large"),
		Key:    aws.String("bigfile.bin"),
	})
	require.NoError(t, err, "pull-through GET must succeed for large object")
	defer getResp.Body.Close()

	got, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	assert.Equal(t, payload, got, "pull-through must return bytes-identical content for large object")

	// Second GET: served from local cache, no upstream needed.
	getResp2, err := localClient.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("large"),
		Key:    aws.String("bigfile.bin"),
	})
	require.NoError(t, err, "cached GET must succeed")
	defer getResp2.Body.Close()
	got2, err := io.ReadAll(getResp2.Body)
	require.NoError(t, err)
	assert.Equal(t, payload, got2, "cached object must be bytes-identical to original")
}
