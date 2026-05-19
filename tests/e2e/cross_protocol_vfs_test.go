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

// vfsStatCall performs a VFS stat operation via the admin API on tgt's writable node.
func vfsStatCall(t *testing.T, endpoint string, req map[string]string, resp *vfsStatResponse) {
	t.Helper()

	body, err := json.Marshal(req)
	require.NoError(t, err)

	httpResp, err := http.Post(endpoint+"/admin/debug/vfs/stat", "application/json", bytes.NewReader(body)) //nolint:noctx
	require.NoError(t, err)
	defer httpResp.Body.Close()

	respBody, err := io.ReadAll(httpResp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, httpResp.StatusCode, "VFS stat failed: %s", string(respBody))

	err = json.Unmarshal(respBody, resp)
	require.NoError(t, err)
}

// TestCrossProtocolE2E exercises S3 ↔ VFS coherency: an S3 PUT must be
// observable via the admin /admin/debug/vfs/stat probe, and an S3 DELETE
// must make the VFS view disappear. Shared single + shared cluster fixtures
// — each sub-test scopes itself with a uniqueBucket.
func TestCrossProtocolE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runCrossProtocolCases(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runCrossProtocolCases(t, newSharedClusterS3Target(t))
	})
}

func runCrossProtocolCases(t *testing.T, tgt s3Target) {
	t.Helper()
	ctx := context.Background()
	cli := tgt.pickNode(0)
	endpoint := tgt.endpoint(0)

	t.Run("S3PutVFSStat", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "putvfs")

		key := "test-file.txt"
		content := "hello, world"
		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte(content)),
		})
		require.NoError(t, err)

		head, err := cli.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		assert.NotNil(t, head.ContentLength)
		assert.Equal(t, int64(len(content)), *head.ContentLength)

		var statResp vfsStatResponse
		vfsStatCall(t, endpoint, map[string]string{
			"volume_name": bucket,
			"file_path":   key,
		}, &statResp)

		assert.True(t, statResp.Exists, "VFS should see file after S3 PUT")
		assert.Equal(t, key, statResp.Name)
		assert.Equal(t, int64(len(content)), statResp.Size)
		assert.Equal(t, false, statResp.IsDir)
	})

	t.Run("S3DeleteVFSESTALE", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "delvfs")

		key := "test-file.txt"
		content := "hello, world"
		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte(content)),
		})
		require.NoError(t, err)

		head, err := cli.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		assert.NotNil(t, head)

		statReq := map[string]string{
			"volume_name": bucket,
			"file_path":   key,
		}
		var statResp vfsStatResponse
		vfsStatCall(t, endpoint, statReq, &statResp)
		assert.True(t, statResp.Exists, "VFS should see file before delete")

		_, err = cli.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)

		// Eventual consistency window (Raft commit + cache invalidation).
		time.Sleep(200 * time.Millisecond)

		_, err = cli.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		assert.Error(t, err, "S3 HEAD should fail after delete")

		vfsStatCall(t, endpoint, statReq, &statResp)
		assert.False(t, statResp.Exists, "VFS should not see deleted file")
	})
}
