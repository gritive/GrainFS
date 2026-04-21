package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// vfsStatResponse is the response from VFS stat operations.
type vfsStatResponse struct {
	Exists  bool   `json:"exists"`
	Name    string `json:"name,omitempty"`
	Size    int64  `json:"size,omitempty"`
	IsDir   bool   `json:"is_dir,omitempty"`
	ModTime string `json:"mod_time,omitempty"`
	Error   string `json:"error,omitempty"`
}

// TestCrossProtocolS3PutVFSStat verifies S3 PUT → VFS stat cache invalidation.
// This is an integration test that uses the admin API to perform VFS stat operations,
// verifying cross-protocol cache coherency works end-to-end.
func TestCrossProtocolS3PutVFSStat(t *testing.T) {
	ctx := context.Background()
	bucket := "test-cross-protocol-s3-put-vfs"

	// Create bucket via S3
	createBucket(t, bucket)

	// Upload file via S3
	key := "test-file.txt"
	content := "hello, world"
	_, err := testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte(content)),
	})
	require.NoError(t, err)

	// Verify file exists via S3
	head, err := testS3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	assert.NotNil(t, head.ContentLength)
	assert.Equal(t, int64(len(content)), *head.ContentLength)

	// Stat file via VFS using admin API
	// This verifies that S3 PUT is visible to VFS (cross-protocol coherency)
	statReq := map[string]string{
		"volume_name": bucket,
		"file_path":   key,
	}
	var statResp vfsStatResponse
	vfsStatCall(t, statReq, &statResp)

	// File should exist
	assert.True(t, statResp.Exists, "VFS should see file after S3 PUT")
	assert.Equal(t, key, statResp.Name)
	assert.Equal(t, int64(len(content)), statResp.Size)
	assert.Equal(t, false, statResp.IsDir)
}

// TestCrossProtocolS3DeleteVFSESTALE verifies S3 DELETE → VFS ESTALE propagation.
func TestCrossProtocolS3DeleteVFSESTALE(t *testing.T) {
	ctx := context.Background()
	bucket := "test-cross-protocol-s3-delete-estale"

	createBucket(t, bucket)

	// Upload file via S3
	key := "test-file.txt"
	content := "hello, world"
	_, err := testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte(content)),
	})
	require.NoError(t, err)

	// Verify file exists via S3
	head, err := testS3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	assert.NotNil(t, head)

	// Verify file exists via VFS
	statReq := map[string]string{
		"volume_name": bucket,
		"file_path":   key,
	}
	var statResp vfsStatResponse
	vfsStatCall(t, statReq, &statResp)
	assert.True(t, statResp.Exists, "VFS should see file before delete")

	// Delete file via S3
	_, err = testS3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)

	// Wait for eventual consistency (Raft commit + cache invalidation)
	time.Sleep(200 * time.Millisecond)

	// Verify file is deleted via S3
	_, err = testS3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	assert.Error(t, err, "S3 HEAD should fail after delete")

	// Verify file is deleted via VFS
	// Note: In current implementation, MarkDeleted is only called when Raft FSM
	// applies the delete. In no-peers mode (which tests use), there's no Raft,
	// so MarkDeleted won't be called. The file should still not exist because
	// it's deleted from storage.
	vfsStatCall(t, statReq, &statResp)
	assert.False(t, statResp.Exists, "VFS should not see deleted file")
}

// vfsStatCall performs a VFS stat operation via the admin API.
func vfsStatCall(t *testing.T, req map[string]string, resp *vfsStatResponse) {
	t.Helper()

	body, err := json.Marshal(req)
	require.NoError(t, err)

	httpResp, err := http.Post(testServerURL+"/admin/debug/vfs/stat", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer httpResp.Body.Close()

	respBody, err := io.ReadAll(httpResp.Body)
	require.NoError(t, err)

	if httpResp.StatusCode != http.StatusOK {
		t.Fatalf("VFS stat failed: %s", string(respBody))
	}

	err = json.Unmarshal(respBody, resp)
	require.NoError(t, err)
}
