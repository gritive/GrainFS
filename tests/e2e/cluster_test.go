package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getBinary() string {
	binary := os.Getenv("GRAINFS_BINARY")
	if binary == "" {
		binary = "../../bin/grainfs"
	}
	return binary
}

// TestNoPeersRestartPersistenceE2E exercises stop+restart on the same dataDir
// — single-binary semantics. Cluster has no analogue (raft handles
// process-restart differently), so this is single-node-only by nature.
func runNoPeersRestartPersistence(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runNoPeersRestartPersistenceCases(t)
	})
}

func runNoPeersRestartPersistenceCases(t *testing.T) {
	t.Helper()
	dir, err := os.MkdirTemp("", "grainfs-cluster-e2e-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	binary := getBinary()

	// Shared encryption key — restart on the same dataDir requires the
	// IAM secrets to round-trip across processes.
	encKeyFile := makeSharedEncryptionKeyFile(t)

	// Step 1: Start in no-peers mode, create some data
	port1 := freePort()
	cmd1 := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port1),
		"--encryption-key-file", encKeyFile,
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	)
	cmd1.Stdout = os.Stdout
	cmd1.Stderr = os.Stderr
	require.NoError(t, cmd1.Start())

	endpoint1 := fmt.Sprintf("http://127.0.0.1:%d", port1)
	waitForPort(t, port1, 30*time.Second)
	bootstrap, _ := bootstrapAdminViaUDSAnyResult(t, []string{dir}, 30*time.Second)
	ak, sk := bootstrap.AccessKey, bootstrap.SecretKey
	client1 := s3ClientFor(endpoint1, ak, sk)

	ctx := context.Background()

	require.NoError(t, adminCreateBucketWithPolicyAttachAny([]string{dir}, bootstrap.SAID, "persist-test", 30*time.Second))

	testData := map[string]string{
		"file1.txt":           "hello from local",
		"docs/readme.md":      "# GrainFS",
		"data/nested/obj.bin": "binary content",
	}

	for key, content := range testData {
		require.Eventually(t, func() bool {
			return tryPutObject(ctx, client1, "persist-test", key, []byte(content)) == nil
		}, 30*time.Second, 500*time.Millisecond, "put %s", key)
	}

	listOut, err := client1.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("persist-test"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Contents, len(testData))

	// Stop server and wait for full teardown
	terminateProcess(cmd1)
	time.Sleep(200 * time.Millisecond) // let OS release file locks

	// Step 2: Restart on a different port and verify data is intact
	port2 := freePort()
	cmd2 := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port2),
		"--encryption-key-file", encKeyFile,
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	)
	cmd2.Stdout = os.Stdout
	cmd2.Stderr = os.Stderr
	require.NoError(t, cmd2.Start())
	defer func() {
		terminateProcess(cmd2)
	}()

	endpoint2 := fmt.Sprintf("http://127.0.0.1:%d", port2)
	waitForPort(t, port2, 30*time.Second)
	client2 := s3ClientFor(endpoint2, ak, sk)

	var listOut2 *s3.ListObjectsV2Output
	require.Eventually(t, func() bool {
		attemptCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		listOut2, err = client2.ListObjectsV2(attemptCtx, &s3.ListObjectsV2Input{
			Bucket: aws.String("persist-test"),
		}, func(o *s3.Options) {
			o.RetryMaxAttempts = 1
		})
		return err == nil && len(listOut2.Contents) == len(testData)
	}, 60*time.Second, 1*time.Second, "list objects after restart: %v", err)
	assert.Len(t, listOut2.Contents, len(testData), "all objects should survive restart")

	for key, expectedContent := range testData {
		getOut, err := client2.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String("persist-test"),
			Key:    aws.String(key),
		})
		require.NoError(t, err, "get %s after restart", key)
		body, _ := io.ReadAll(getOut.Body)
		getOut.Body.Close()
		assert.Equal(t, expectedContent, string(body), "content mismatch for %s", key)
	}

	// Verify new writes work after restart
	require.Eventually(t, func() bool {
		return tryPutObject(ctx, client2, "persist-test", "post-restart.txt", []byte("new data after restart")) == nil
	}, 30*time.Second, 500*time.Millisecond, "put post-restart.txt")

	getOut, err := client2.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("persist-test"),
		Key:    aws.String("post-restart.txt"),
	})
	require.NoError(t, err)
	body, _ := io.ReadAll(getOut.Body)
	getOut.Body.Close()
	assert.Equal(t, "new data after restart", string(body))
}

// TestNoPeersMultipartE2E exercises multipart against a single-binary
// no-peers server. The cluster shape's multipart coverage lives in
// TestMultipartE2E/Cluster4Node; this case stays single-only by design.
func runNoPeersMultipart(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runNoPeersMultipartCases(t)
	})
}

func runNoPeersMultipartCases(t *testing.T) {
	t.Helper()
	dir, err := os.MkdirTemp("", "grainfs-cluster-mp-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	binary := getBinary()
	port := freePort()

	cmd := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	defer func() {
		terminateProcess(cmd)
	}()

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(t, port, 30*time.Second)
	bootstrap, _ := bootstrapAdminViaUDSAnyResult(t, []string{dir}, 30*time.Second)
	client := s3ClientFor(endpoint, bootstrap.AccessKey, bootstrap.SecretKey)

	ctx := context.Background()

	require.NoError(t, adminCreateBucketWithPolicyAttachAny([]string{dir}, bootstrap.SAID, "mp-cluster", 30*time.Second))
	waitForS3Write(t, client, "mp-cluster", "__grainfs_e2e_ready", 30*time.Second)
	_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String("mp-cluster"),
		Key:    aws.String("__grainfs_e2e_ready"),
	})

	// Multipart upload
	key := "multipart-cluster.bin"
	part1Data := bytes.Repeat([]byte("X"), 5*1024*1024)
	part2Data := bytes.Repeat([]byte("Y"), 512)

	initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String("mp-cluster"),
		Key:    aws.String(key),
	})
	require.NoError(t, err)

	p1, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("mp-cluster"),
		Key:        aws.String(key),
		UploadId:   initOut.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(part1Data),
	})
	require.NoError(t, err)

	p2, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("mp-cluster"),
		Key:        aws.String(key),
		UploadId:   initOut.UploadId,
		PartNumber: aws.Int32(2),
		Body:       bytes.NewReader(part2Data),
	})
	require.NoError(t, err)

	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String("mp-cluster"),
		Key:      aws.String(key),
		UploadId: initOut.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: p1.ETag},
				{PartNumber: aws.Int32(2), ETag: p2.ETag},
			},
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("mp-cluster"),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	defer getOut.Body.Close()

	body, _ := io.ReadAll(getOut.Body)
	expected := append(part1Data, part2Data...)
	assert.Equal(t, expected, body)
}

// TestClusterMultipartListFanoutE2E verifies that an incomplete multipart
// upload created on the leader is visible from every cluster node — i.e.
// metadata fan-out works. Cluster-only by nature: the case probes per-node
// visibility, which has no single-node analogue. Runs on the shared cluster.
//
// The plain multipart-list assertion is already exercised by
// TestMultipartE2E/Cluster4Node/List; this test adds the per-node fanout
// assertion on top of the same fixture.
func TestClusterMultipartListFanoutE2E(t *testing.T) {
	t.Run("Cluster4Node", func(t *testing.T) {
		runMultipartListFanoutCases(t, newSharedClusterS3Target(t))
	})
}

func runMultipartListFanoutCases(t *testing.T, tgt s3Target) {
	t.Helper()
	require.True(t, tgt.isCluster, "multipart list fanout requires cluster fixture")

	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	bucket := tgt.uniqueBucket(t, "mpfanout")
	// Any node is fine — cluster forwards writes to the owning peer. We use
	// tgt.pickNode(0) for the create/fixture and assert visibility from
	// every node below.
	driver := tgt.pickNode(0)
	waitForMultipartListingCreate(t, ctx, driver, bucket, multipartListingKey, 120*time.Second)
	fixture := createIncompleteMultipartListingFixture(t, ctx, driver, bucket, "cluster-fanout-part")

	for i := 0; i < tgt.nodes; i++ {
		t.Run(fmt.Sprintf("node-%d", i+1), func(t *testing.T) {
			assertMultipartListingFeature(t, ctx, tgt.pickNode(i), fixture, true)
		})
	}
}

// TestNoPeersE2E groups single-node (no-peers) behaviors.
func TestNoPeersE2E(t *testing.T) {
	t.Run("Multipart", runNoPeersMultipart)
	t.Run("RestartPersistence", runNoPeersRestartPersistence)
}
