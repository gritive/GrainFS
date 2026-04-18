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

	binary := getBinary()

	// Step 1: Start in solo mode, create some data
	port1 := freePort()
	cmd1 := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port1),
		"--snapshot-interval", "0", // disable auto-snapshots for determinism
		"--nfs4-port", "0",
	)
	cmd1.Stdout = os.Stdout
	cmd1.Stderr = os.Stderr
	require.NoError(t, cmd1.Start())

	endpoint1 := fmt.Sprintf("http://127.0.0.1:%d", port1)
	waitForPort(port1, 10*time.Second)
	client1 := newS3Client(endpoint1)

	ctx := context.Background()

	_, err = client1.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("persist-test"),
	})
	require.NoError(t, err)

	testData := map[string]string{
		"file1.txt":           "hello from solo",
		"docs/readme.md":      "# GrainFS",
		"data/nested/obj.bin": "binary content",
	}

	for key, content := range testData {
		_, err = client1.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("persist-test"),
			Key:    aws.String(key),
			Body:   strings.NewReader(content),
		})
		require.NoError(t, err, "put %s", key)
	}

	listOut, err := client1.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("persist-test"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Contents, len(testData))

	// Stop server and wait for full teardown
	cmd1.Process.Kill()
	cmd1.Wait()
	time.Sleep(200 * time.Millisecond) // let OS release file locks

	// Step 2: Restart on a different port and verify data is intact
	port2 := freePort()
	cmd2 := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port2),
		"--snapshot-interval", "0",
		"--nfs4-port", "0",
	)
	cmd2.Stdout = os.Stdout
	cmd2.Stderr = os.Stderr
	require.NoError(t, cmd2.Start())
	defer func() {
		cmd2.Process.Kill()
		cmd2.Wait()
	}()

	endpoint2 := fmt.Sprintf("http://127.0.0.1:%d", port2)
	waitForPort(port2, 10*time.Second)
	client2 := newS3Client(endpoint2)

	listOut2, err := client2.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("persist-test"),
	})
	require.NoError(t, err)
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
	_, err = client2.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("persist-test"),
		Key:    aws.String("post-restart.txt"),
		Body:   strings.NewReader("new data after restart"),
	})
	require.NoError(t, err)

	getOut, err := client2.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("persist-test"),
		Key:    aws.String("post-restart.txt"),
	})
	require.NoError(t, err)
	body, _ := io.ReadAll(getOut.Body)
	getOut.Body.Close()
	assert.Equal(t, "new data after restart", string(body))
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
		"--nfs4-port", "0",
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

