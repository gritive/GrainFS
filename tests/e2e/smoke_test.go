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
	"github.com/stretchr/testify/require"
)

// TestSmoke_DeploymentVerification is a fast smoke test to verify deployment.
// This test should complete in under 2 minutes and covers the critical path.
func TestSmoke_DeploymentVerification(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}

	dir, err := os.MkdirTemp("", "grainfs-smoke-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	binary := getBinary()
	port := freePort()

	// Start server
	cmd := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs4-port", "0",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	defer cmd.Process.Kill()

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(port, 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := newS3Client(endpoint)

	// Test 1: Health check
	t.Run("HealthCheck", func(t *testing.T) {
		// Verify server is responding
		resp, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
		require.NoError(t, err, "health check should succeed")
		require.NotNil(t, resp, "health check response should not be nil")
	})

	// Test 2: Create bucket
	t.Run("CreateBucket", func(t *testing.T) {
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String("smoke-test"),
		})
		require.NoError(t, err, "create bucket should succeed")
	})

	// Test 3: Put object
	t.Run("PutObject", func(t *testing.T) {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("smoke-test"),
			Key:    aws.String("test-object"),
			Body:   strings.NewReader("smoke test data"),
		})
		require.NoError(t, err, "put object should succeed")
	})

	// Test 4: Get object
	t.Run("GetObject", func(t *testing.T) {
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String("smoke-test"),
			Key:    aws.String("test-object"),
		})
		require.NoError(t, err, "get object should succeed")
		defer resp.Body.Close()

		content, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "read object body should succeed")
		require.Equal(t, "smoke test data", string(content), "object content should match")
	})

	// Test 5: Delete object
	t.Run("DeleteObject", func(t *testing.T) {
		_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String("smoke-test"),
			Key:    aws.String("test-object"),
		})
		require.NoError(t, err, "delete object should succeed")
	})

	// Test 6: List objects (should be empty)
	t.Run("ListObjects", func(t *testing.T) {
		resp, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String("smoke-test"),
		})
		require.NoError(t, err, "list objects should succeed")
		require.Len(t, resp.Contents, 0, "bucket should be empty after delete")
	})

	t.Log("✅ Smoke test passed - deployment verified")
}
