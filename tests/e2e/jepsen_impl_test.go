package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func TestJepsen_RaftCluster_ConcurrentWrites(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("skipping Jepsen test in short mode")
	}

	dir, err := os.MkdirTemp("", "grainfs-jepsen-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	binary := getBinary()
	port := freePort()

	// Start no-peers server
	cmd := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--snapshot-interval", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	defer terminateProcess(cmd)

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(t, port, 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Create test bucket
	client := newS3Client(endpoint)
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("jepsen-test"),
	})
	require.NoError(t, err)
	waitForS3Write(t, client, "jepsen-test", "__grainfs_e2e_ready", 30*time.Second)

	// Run Jepsen test: 10 clients, 100 ops each
	t.Log("Starting concurrent writes with 10 clients...")
	runner := NewJepsenTestRunner(endpoint, 10, 100)
	errors := runner.RunConcurrentPuts(ctx, "jepsen-test", "conflict-key")

	// All operations should succeed
	for i, err := range errors {
		require.NoError(t, err, "client %d put should succeed", i)
	}

	t.Log("✓ All concurrent writes succeeded")

	// Verify linearizability: all clients see same value
	t.Log("Verifying linearizability...")
	runner.VerifyLinearizable(ctx, t, "jepsen-test", "conflict-key")

	t.Log("✅ Jepsen test passed - linearizability verified")
}
