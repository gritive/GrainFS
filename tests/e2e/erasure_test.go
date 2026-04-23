package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shardDir returns the on-disk shard directory for a given bucket/key.
// Must match the logic in erasure.ECBackend.ShardDir.
func shardDir(dataDir, bucket, key string) string {
	h := sha256.Sum256([]byte(key))
	return filepath.Join(dataDir, "data", bucket, ".ec", hex.EncodeToString(h[:]))
}

// startECServer starts a grainfs server with --ec enabled and returns
// the S3 client, data directory, and cleanup function.
func startECServer(t *testing.T) (*s3.Client, string, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "grainfs-ec-e2e-*")
	require.NoError(t, err)

	binary := getBinary()
	port := freePort()

	cmd := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--cluster-ec",
		"--nfs4-port", "0",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(t, port, 5*time.Second)

	client := newS3Client(endpoint)

	cleanup := func() {
		cmd.Process.Kill()
		cmd.Wait()
		os.RemoveAll(dir)
	}

	return client, dir, cleanup
}

// startECServerWithScrub starts an EC server with a custom scrub interval.
// Returns S3 client, data dir, base URL, and cleanup.
func startECServerWithScrub(t *testing.T, scrubInterval time.Duration) (*s3.Client, string, string, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "grainfs-ec-scrub-e2e-*")
	require.NoError(t, err)

	binary := getBinary()
	port := freePort()

	cmd := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--cluster-ec",
		"--no-encryption",
		"--scrub-interval", scrubInterval.String(),
		"--nfs4-port", "0",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(t, port, 5*time.Second)

	client := newS3Client(baseURL)

	cleanup := func() {
		cmd.Process.Kill()
		cmd.Wait()
		os.RemoveAll(dir)
	}

	return client, dir, baseURL, cleanup
}

func TestEC_BasicPutGet(t *testing.T) {
	client, _, cleanup := startECServer(t)
	defer cleanup()

	ctx := context.Background()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("ec-basic"),
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		key     string
		content string
	}{
		{"small_object", "small.txt", "hello erasure coding"},
		{"medium_object", "medium.txt", strings.Repeat("EC test data ", 1000)},
		{"nested_key", "path/to/deep/file.txt", "nested EC content"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String("ec-basic"),
				Key:    aws.String(tt.key),
				Body:   strings.NewReader(tt.content),
			})
			require.NoError(t, err)

			getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String("ec-basic"),
				Key:    aws.String(tt.key),
			})
			require.NoError(t, err)
			defer getOut.Body.Close()

			body, _ := io.ReadAll(getOut.Body)
			assert.Equal(t, tt.content, string(body))
			assert.Equal(t, int64(len(tt.content)), aws.ToInt64(getOut.ContentLength))
		})
	}
}

func TestEC_LargeObject(t *testing.T) {
	client, _, cleanup := startECServer(t)
	defer cleanup()

	ctx := context.Background()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("ec-large"),
	})
	require.NoError(t, err)

	// 5MB object — larger than default shard size
	data := bytes.Repeat([]byte("X"), 5*1024*1024)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("ec-large"),
		Key:    aws.String("large.bin"),
		Body:   bytes.NewReader(data),
	})
	require.NoError(t, err)

	getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("ec-large"),
		Key:    aws.String("large.bin"),
	})
	require.NoError(t, err)
	defer getOut.Body.Close()

	body, _ := io.ReadAll(getOut.Body)
	assert.Equal(t, data, body)
}

func TestEC_ShardRecovery(t *testing.T) {
	t.Skip("ECBackend-specific shard recovery; singleton DistributedBackend does not run EC (k+m > 1). Tracked in TODOS v0.0.4.0 — cluster-mode EC recovery covered by TestE2E_ClusterScrubber_AutoRepair")
	client, dataDir, cleanup := startECServer(t)
	defer cleanup()

	ctx := context.Background()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("ec-recovery"),
	})
	require.NoError(t, err)

	content := bytes.Repeat([]byte("shard recovery test "), 500)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("ec-recovery"),
		Key:    aws.String("recover.bin"),
		Body:   bytes.NewReader(content),
	})
	require.NoError(t, err)

	// Verify shards exist on disk
	dir := shardDir(dataDir, "ec-recovery", "recover.bin")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 6, "should have 6 shards (4 data + 2 parity)")

	// Delete 2 shards (the maximum tolerable with m=2)
	os.Remove(filepath.Join(dir, "00"))
	os.Remove(filepath.Join(dir, "05"))

	// Verify recovery works
	getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("ec-recovery"),
		Key:    aws.String("recover.bin"),
	})
	require.NoError(t, err, "should recover with 2 missing shards")
	defer getOut.Body.Close()

	body, _ := io.ReadAll(getOut.Body)
	assert.Equal(t, content, body, "recovered data should match original")
}

func TestEC_TooManyShardsLost(t *testing.T) {
	t.Skip("ECBackend-specific: singleton DistributedBackend stores objects intact (no local k+m split). Tracked in TODOS v0.0.4.0")
	client, dataDir, cleanup := startECServer(t)
	defer cleanup()

	ctx := context.Background()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("ec-lost"),
	})
	require.NoError(t, err)

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("ec-lost"),
		Key:    aws.String("doomed.bin"),
		Body:   strings.NewReader("this data will be lost"),
	})
	require.NoError(t, err)

	// Delete 3 shards — exceeds m=2
	dir := shardDir(dataDir, "ec-lost", "doomed.bin")
	os.Remove(filepath.Join(dir, "00"))
	os.Remove(filepath.Join(dir, "01"))
	os.Remove(filepath.Join(dir, "02"))

	_, err = client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("ec-lost"),
		Key:    aws.String("doomed.bin"),
	})
	assert.Error(t, err, "should fail when too many shards are missing")
}

func TestEC_MultipartUpload(t *testing.T) {
	client, _, cleanup := startECServer(t)
	defer cleanup()

	ctx := context.Background()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("ec-multipart"),
	})
	require.NoError(t, err)

	key := "multipart-ec.bin"
	part1Data := bytes.Repeat([]byte("A"), 1024)
	part2Data := bytes.Repeat([]byte("B"), 512)

	initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String("ec-multipart"),
		Key:    aws.String(key),
	})
	require.NoError(t, err)

	p1, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("ec-multipart"),
		Key:        aws.String(key),
		UploadId:   initOut.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(part1Data),
	})
	require.NoError(t, err)

	p2, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("ec-multipart"),
		Key:        aws.String(key),
		UploadId:   initOut.UploadId,
		PartNumber: aws.Int32(2),
		Body:       bytes.NewReader(part2Data),
	})
	require.NoError(t, err)

	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String("ec-multipart"),
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
		Bucket: aws.String("ec-multipart"),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	defer getOut.Body.Close()

	body, _ := io.ReadAll(getOut.Body)
	expected := append(part1Data, part2Data...)
	assert.Equal(t, expected, body)
}

func TestEC_BucketOperations(t *testing.T) {
	client, _, cleanup := startECServer(t)
	defer cleanup()

	ctx := context.Background()

	// Create, Head, List, Delete
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("ec-bucket-ops"),
	})
	require.NoError(t, err)

	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String("ec-bucket-ops"),
	})
	require.NoError(t, err)

	listOut, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
	require.NoError(t, err)
	found := false
	for _, b := range listOut.Buckets {
		if aws.ToString(b.Name) == "ec-bucket-ops" {
			found = true
		}
	}
	assert.True(t, found)

	_, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String("ec-bucket-ops"),
	})
	require.NoError(t, err)
}

func TestEC_DeleteAndOverwrite(t *testing.T) {
	client, _, cleanup := startECServer(t)
	defer cleanup()

	ctx := context.Background()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("ec-delover"),
	})
	require.NoError(t, err)

	// Put, Delete, verify gone
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("ec-delover"),
		Key:    aws.String("file.txt"),
		Body:   strings.NewReader("v1"),
	})
	require.NoError(t, err)

	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String("ec-delover"),
		Key:    aws.String("file.txt"),
	})
	require.NoError(t, err)

	_, err = client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("ec-delover"),
		Key:    aws.String("file.txt"),
	})
	assert.Error(t, err)

	// Overwrite
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("ec-delover"),
		Key:    aws.String("over.txt"),
		Body:   strings.NewReader("version1"),
	})
	require.NoError(t, err)

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("ec-delover"),
		Key:    aws.String("over.txt"),
		Body:   strings.NewReader("version2"),
	})
	require.NoError(t, err)

	getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("ec-delover"),
		Key:    aws.String("over.txt"),
	})
	require.NoError(t, err)
	defer getOut.Body.Close()

	body, _ := io.ReadAll(getOut.Body)
	assert.Equal(t, "version2", string(body))
}

// TestScrubber_AutoRepair verifies that the background scrubber detects a
// deleted shard and automatically repairs it, restoring the full shard set.
func TestScrubber_AutoRepair(t *testing.T) {
	t.Skip("ECBackend-specific scrubber test; cluster-mode scrubber covered by TestE2E_ClusterScrubber_AutoRepair. Tracked in TODOS v0.0.4.0")
	// Use a short scrub interval so the test doesn't take long
	const scrubInterval = 150 * time.Millisecond
	client, dataDir, baseURL, cleanup := startECServerWithScrub(t, scrubInterval)
	defer cleanup()

	ctx := context.Background()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("scrub-test"),
	})
	require.NoError(t, err)

	// Use content large enough to trigger EC encoding (not plain storage)
	content := bytes.Repeat([]byte("scrubber auto-repair test data "), 200) // ~6KB
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("scrub-test"),
		Key:    aws.String("file.bin"),
		Body:   bytes.NewReader(content),
	})
	require.NoError(t, err)

	// Verify 6 shards exist
	dir := shardDir(dataDir, "scrub-test", "file.bin")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 6, "expected 6 EC shards")

	// Delete shard 1 to simulate disk failure
	require.NoError(t, os.Remove(filepath.Join(dir, "01")))
	entries, _ = os.ReadDir(dir)
	require.Len(t, entries, 5, "shard 01 should be deleted")

	// Wait for scrubber to detect and repair the missing shard
	deadline := time.Now().Add(3 * time.Second)
	repaired := false
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		entries, _ = os.ReadDir(dir)
		if len(entries) == 6 {
			repaired = true
			break
		}
	}
	assert.True(t, repaired, "scrubber should restore shard 01 within 3s")

	// S3 GET must still succeed
	getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("scrub-test"),
		Key:    aws.String("file.bin"),
	})
	require.NoError(t, err, "S3 GET must succeed after scrub repair")
	defer getOut.Body.Close()
	got, _ := io.ReadAll(getOut.Body)
	assert.Equal(t, content, got, "recovered data must match original")

	// Verify /admin/health/scrub reports repaired > 0
	resp, err := http.Get(baseURL + "/admin/health/scrub")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var stats struct {
		Repaired  int64 `json:"repaired"`
		Available bool  `json:"available"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&stats))
	assert.True(t, stats.Available, "scrubber should be available")
	assert.Greater(t, stats.Repaired, int64(0), "scrubber should have repaired at least 1 object")
}
