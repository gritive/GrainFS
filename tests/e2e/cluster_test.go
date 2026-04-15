package e2e

import (
	"bytes"
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

func TestCluster_SoloRaft_BasicOperations(t *testing.T) {
	dir, err := os.MkdirTemp("", "grainfs-cluster-e2e-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// We need to test cluster mode but without the --peers flag resulting in
	// running with an actual Raft cluster. Instead, we test that the solo
	// server (default mode, no --peers) continues to work perfectly.
	// The solo mode is the baseline; cluster mode is tested via the migrate path.

	// For now, test the migrate command and cluster restart.
	binary := getBinary()
	soloPort := freePort()

	// Step 1: Start in solo mode, create some data
	soloCmd := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", soloPort),
	)
	soloCmd.Stdout = os.Stdout
	soloCmd.Stderr = os.Stderr
	require.NoError(t, soloCmd.Start())

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", soloPort)
	waitForPort(soloPort, 5*time.Second)
	client := newS3Client(endpoint)

	ctx := context.Background()

	// Create bucket and objects in solo mode
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("migrate-test"),
	})
	require.NoError(t, err)

	testData := map[string]string{
		"file1.txt":          "hello from solo",
		"docs/readme.md":     "# GrainFS",
		"data/nested/obj.bin": "binary content",
	}

	for key, content := range testData {
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("migrate-test"),
			Key:    aws.String(key),
			Body:   strings.NewReader(content),
		})
		require.NoError(t, err, "put %s", key)
	}

	// Verify data in solo mode
	listOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("migrate-test"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Contents, len(testData))

	// Stop solo server
	soloCmd.Process.Kill()
	soloCmd.Wait()

	// Step 2: Run migrate
	migrateCmd := exec.Command(binary, "migrate",
		"--data", dir,
		"--node-id", "seed-node-1",
	)
	migrateCmd.Stdout = os.Stdout
	migrateCmd.Stderr = os.Stderr
	err = migrateCmd.Run()
	require.NoError(t, err, "migrate command should succeed")

	// Step 3: Restart in solo mode and verify data is intact
	soloPort2 := freePort()
	soloCmd2 := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", soloPort2),
	)
	soloCmd2.Stdout = os.Stdout
	soloCmd2.Stderr = os.Stderr
	require.NoError(t, soloCmd2.Start())
	defer func() {
		soloCmd2.Process.Kill()
		soloCmd2.Wait()
	}()

	endpoint2 := fmt.Sprintf("http://127.0.0.1:%d", soloPort2)
	waitForPort(soloPort2, 5*time.Second)
	client2 := newS3Client(endpoint2)

	// Verify all data survived migration
	listOut2, err := client2.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("migrate-test"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut2.Contents, len(testData), "all objects should survive migration")

	for key, expectedContent := range testData {
		getOut, err := client2.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String("migrate-test"),
			Key:    aws.String(key),
		})
		require.NoError(t, err, "get %s after migration", key)
		body, _ := io.ReadAll(getOut.Body)
		getOut.Body.Close()
		assert.Equal(t, expectedContent, string(body), "content mismatch for %s", key)
	}

	// Verify we can still create new data
	_, err = client2.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("migrate-test"),
		Key:    aws.String("post-migrate.txt"),
		Body:   strings.NewReader("new data after migration"),
	})
	require.NoError(t, err)

	getOut, err := client2.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("migrate-test"),
		Key:    aws.String("post-migrate.txt"),
	})
	require.NoError(t, err)
	body, _ := io.ReadAll(getOut.Body)
	getOut.Body.Close()
	assert.Equal(t, "new data after migration", string(body))
}

func TestCluster_SoloRaft_Multipart(t *testing.T) {
	dir, err := os.MkdirTemp("", "grainfs-cluster-mp-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	binary := getBinary()
	port := freePort()

	cmd := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	defer func() {
		cmd.Process.Kill()
		cmd.Wait()
	}()

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(port, 5*time.Second)
	client := newS3Client(endpoint)

	ctx := context.Background()

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("mp-cluster"),
	})
	require.NoError(t, err)

	// Multipart upload
	key := "multipart-cluster.bin"
	part1Data := bytes.Repeat([]byte("X"), 1024)
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

