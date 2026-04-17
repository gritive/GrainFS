package e2e

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
		"--nfs-port", "0",
	)
	upCmd.Stdout = os.Stdout
	upCmd.Stderr = os.Stderr
	require.NoError(t, upCmd.Start())
	defer upCmd.Process.Kill()

	waitForPort(upPort, 5*time.Second)
	upEndpoint := fmt.Sprintf("http://127.0.0.1:%d", upPort)
	upClient := newS3Client(upEndpoint)

	// Put an object in the upstream
	_, err = upClient.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("shared")})
	require.NoError(t, err)
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
		"--nfs-port", "0",
		"--upstream", upEndpoint,
	)
	localCmd.Stdout = os.Stdout
	localCmd.Stderr = os.Stderr
	require.NoError(t, localCmd.Start())
	defer localCmd.Process.Kill()

	waitForPort(localPort, 5*time.Second)
	localClient := newS3Client(fmt.Sprintf("http://127.0.0.1:%d", localPort))

	// Bucket must exist on local (pull-through creates it if needed)
	_, err = localClient.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("shared")})
	require.NoError(t, err)

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
