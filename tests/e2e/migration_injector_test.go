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

// TestMigrationInjector_CopiesFromSourceToDest verifies the `grainfs migrate inject`
// command copies all objects from a source GrainFS to a destination GrainFS.
func TestMigrationInjector_CopiesFromSourceToDest(t *testing.T) {
	binary := getBinary()
	ctx := context.Background()

	// --- Source GrainFS ---
	srcDir, err := os.MkdirTemp("", "grainfs-migrate-src-*")
	require.NoError(t, err)
	defer os.RemoveAll(srcDir)

	srcPort := freePort()
	srcCmd := exec.Command(binary, "serve",
		"--data", srcDir,
		"--port", fmt.Sprintf("%d", srcPort),
		"--nfs-port", "0",
	)
	srcCmd.Stdout = os.Stdout
	srcCmd.Stderr = os.Stderr
	require.NoError(t, srcCmd.Start())
	defer srcCmd.Process.Kill()
	waitForPort(srcPort, 5*time.Second)

	srcEndpoint := fmt.Sprintf("http://127.0.0.1:%d", srcPort)
	srcClient := newS3Client(srcEndpoint)

	_, err = srcClient.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("data")})
	require.NoError(t, err)
	_, err = srcClient.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("data"),
		Key:    aws.String("hello.txt"),
		Body:   strings.NewReader("hello from source"),
	})
	require.NoError(t, err)
	_, err = srcClient.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("data"),
		Key:    aws.String("world.txt"),
		Body:   strings.NewReader("world from source"),
	})
	require.NoError(t, err)

	// --- Destination GrainFS ---
	dstDir, err := os.MkdirTemp("", "grainfs-migrate-dst-*")
	require.NoError(t, err)
	defer os.RemoveAll(dstDir)

	dstPort := freePort()
	dstCmd := exec.Command(binary, "serve",
		"--data", dstDir,
		"--port", fmt.Sprintf("%d", dstPort),
		"--nfs-port", "0",
	)
	dstCmd.Stdout = os.Stdout
	dstCmd.Stderr = os.Stderr
	require.NoError(t, dstCmd.Start())
	defer dstCmd.Process.Kill()
	waitForPort(dstPort, 5*time.Second)

	dstEndpoint := fmt.Sprintf("http://127.0.0.1:%d", dstPort)
	dstClient := newS3Client(dstEndpoint)

	// --- Run migration injector ---
	injectCmd := exec.Command(binary, "migrate", "inject",
		"--src", srcEndpoint,
		"--dst", dstEndpoint,
	)
	injectCmd.Stdout = os.Stdout
	injectCmd.Stderr = os.Stderr
	require.NoError(t, injectCmd.Run(), "migrate inject must succeed")

	// Verify objects are in destination
	for _, key := range []string{"hello.txt", "world.txt"} {
		getResp, err := dstClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String("data"),
			Key:    aws.String(key),
		})
		require.NoError(t, err, "object %s must exist in destination after migration", key)
		body, _ := io.ReadAll(getResp.Body)
		getResp.Body.Close()
		assert.True(t, strings.Contains(string(body), "source"), "content must match source")
	}
}
