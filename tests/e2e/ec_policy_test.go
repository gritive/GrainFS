package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
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

// startECServerWithPort starts an EC server and returns the port as well.
func startECServerWithPort(t *testing.T) (*s3.Client, string, int, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "grainfs-ecpol-e2e-*")
	require.NoError(t, err)

	binary := getBinary()
	port := freePort()

	cmd := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--ec",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(port, 5*time.Second)

	client := newS3Client(endpoint)

	cleanup := func() {
		cmd.Process.Kill()
		cmd.Wait()
		os.RemoveAll(dir)
	}

	return client, dir, port, cleanup
}

func TestBucketECPolicy_Toggle(t *testing.T) {
	client, dataDir, port, cleanup := startECServerWithPort(t)
	defer cleanup()

	ctx := context.Background()

	// Create a bucket (default: EC enabled)
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("no-ec-bucket"),
	})
	require.NoError(t, err)

	// Disable EC for this bucket via PUT /:bucket?ec=false
	req, _ := http.NewRequest(http.MethodPut,
		fmt.Sprintf("http://127.0.0.1:%d/no-ec-bucket?ec=false", port), nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Put object - should NOT be EC-encoded
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("no-ec-bucket"),
		Key:    aws.String("plain.txt"),
		Body:   strings.NewReader("no erasure coding here"),
	})
	require.NoError(t, err)

	// Verify no .ec directory
	ecDir := shardDir(dataDir, "no-ec-bucket", "plain.txt")
	_, err = os.Stat(ecDir)
	assert.True(t, os.IsNotExist(err), "EC directory should not exist for non-EC bucket")

	// Verify object is still readable
	getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("no-ec-bucket"),
		Key:    aws.String("plain.txt"),
	})
	require.NoError(t, err)
	defer getOut.Body.Close()

	body, _ := io.ReadAll(getOut.Body)
	assert.Equal(t, "no erasure coding here", string(body))

	// Create EC-enabled bucket (default behavior)
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("ec-bucket"),
	})
	require.NoError(t, err)

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("ec-bucket"),
		Key:    aws.String("coded.txt"),
		Body:   strings.NewReader("erasure coded data"),
	})
	require.NoError(t, err)

	// Verify .ec directory exists with shards
	ecDir2 := shardDir(dataDir, "ec-bucket", "coded.txt")
	entries, err := os.ReadDir(ecDir2)
	require.NoError(t, err)
	assert.Len(t, entries, 6, "should have 6 shards for EC bucket")
}
