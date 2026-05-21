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
	"github.com/onsi/ginkgo/v2"
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
func vfsStatCall(t testing.TB, endpoint string, req map[string]string, resp *vfsStatResponse) {
	t.Helper()

	body, err := json.Marshal(req)
	require.NoError(t, err)

	httpResp, err := http.Post(endpoint+"/admin/debug/vfs/stat", "application/json", bytes.NewReader(body)) //nolint:noctx
	require.NoError(t, err)
	ginkgo.DeferCleanup(httpResp.Body.Close)

	respBody, err := io.ReadAll(httpResp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, httpResp.StatusCode, "VFS stat failed: %s", string(respBody))

	err = json.Unmarshal(respBody, resp)
	require.NoError(t, err)
}

// Cross-protocol specs exercise S3 ↔ VFS coherency: an S3 PUT must be
// observable via the admin /admin/debug/vfs/stat probe, and an S3 DELETE
// must make the VFS view disappear. Shared single + shared cluster fixtures
// — each sub-test scopes itself with a uniqueBucket.
var _ = ginkgo.Describe("Cross protocol VFS coherency", func() {
	describeCrossProtocolContext("SingleNode", func() s3Target {
		return newSingleNodeS3Target()
	})

	describeCrossProtocolContext("Cluster4Node", func() s3Target {
		return newSharedClusterS3Target(ginkgo.GinkgoTB())
	})
})

func describeCrossProtocolContext(name string, factory func() s3Target) {
	ginkgo.Context(name, func() {
		var (
			tgt      s3Target
			cli      *s3.Client
			endpoint string
		)

		ginkgo.BeforeEach(func() {
			tgt = factory()
			cli = tgt.pickNode(0)
			endpoint = tgt.endpoint(0)
		})

		runCrossProtocolCases(func() s3Target { return tgt }, func() *s3.Client { return cli }, func() string { return endpoint })
	})
}

func runCrossProtocolCases(getTgt func() s3Target, getClient func() *s3.Client, getEndpoint func() string) {
	ginkgo.It("makes S3 PUTs visible through VFS stat", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		cli := getClient()
		endpoint := getEndpoint()
		ctx := context.Background()
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

	ginkgo.It("removes deleted S3 objects from VFS stat", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		cli := getClient()
		endpoint := getEndpoint()
		ctx := context.Background()
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
