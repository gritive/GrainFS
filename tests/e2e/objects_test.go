// Object-level S3 API e2e (target table-driven).
//
// The same case set runs against a single-node fixture and a 4-node cluster
// fixture. Bucket names are prefixed with tgt.name to avoid collisions.
package e2e

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func stringReader(s string) io.Reader {
	return strings.NewReader(s)
}

func TestObjectsE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runObjectCases(t, newSingleNodeS3Target())
	})

	t.Run("Cluster4Node", func(t *testing.T) {
		skipIfShort(t, "cluster fixture not booted in -short mode")
		runObjectCases(t, newSharedClusterS3Target(t))
	})
}

func runObjectCases(t *testing.T, tgt s3Target) {
	client := tgt.pickNode(0)

	t.Run("PutAndGet", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.name + "-obj-putget"
		tgt.createBkt(t, bucket)

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String("hello.txt"),
			Body:        stringReader("hello grainfs"),
			ContentType: aws.String("text/plain"),
		})
		require.NoError(t, err)

		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("hello.txt"),
		})
		require.NoError(t, err)
		defer out.Body.Close()

		body, err := io.ReadAll(out.Body)
		require.NoError(t, err)
		assert.Equal(t, "hello grainfs", string(body))
		assert.Equal(t, "text/plain", aws.ToString(out.ContentType))
		assert.Equal(t, int64(13), aws.ToInt64(out.ContentLength))
	})

	t.Run("Head", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.name + "-obj-head"
		tgt.createBkt(t, bucket)

		client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("meta.txt"),
			Body:   stringReader("metadata test"),
		})

		out, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("meta.txt"),
		})
		require.NoError(t, err)
		assert.Equal(t, int64(13), aws.ToInt64(out.ContentLength))
		assert.NotEmpty(t, aws.ToString(out.ETag))
	})

	t.Run("HeadNotFound", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.name + "-obj-headnf"
		tgt.createBkt(t, bucket)

		_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("nope.txt"),
		})
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "NotFound", apiErr.ErrorCode())
	})

	t.Run("Delete", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.name + "-obj-del"
		tgt.createBkt(t, bucket)

		client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("to-delete.txt"),
			Body:   stringReader("gone"),
		})

		_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("to-delete.txt"),
		})
		require.NoError(t, err)

		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("to-delete.txt"),
		})
		require.Error(t, err)
	})

	t.Run("DeleteNonexistent", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.name + "-obj-delnone"
		tgt.createBkt(t, bucket)

		_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("never-existed.txt"),
		})
		require.NoError(t, err)
	})

	t.Run("PutToNonexistentBucket", func(t *testing.T) {
		ctx := context.Background()

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(tgt.name + "-nope-bucket"),
			Key:    aws.String("file.txt"),
			Body:   stringReader("data"),
		})
		require.Error(t, err)
	})

	t.Run("GetNonexistent", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.name + "-obj-getnone"
		tgt.createBkt(t, bucket)

		_, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("nope.txt"),
		})
		require.Error(t, err)
	})

	t.Run("Overwrite", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.name + "-obj-overwrite"
		tgt.createBkt(t, bucket)

		client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("file.txt"),
			Body:   stringReader("v1"),
		})
		client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("file.txt"),
			Body:   stringReader("version2"),
		})

		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("file.txt"),
		})
		require.NoError(t, err)
		defer out.Body.Close()

		body, _ := io.ReadAll(out.Body)
		assert.Equal(t, "version2", string(body))
		assert.Equal(t, int64(8), aws.ToInt64(out.ContentLength))
	})

	t.Run("NestedKey", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.name + "-obj-nested"
		tgt.createBkt(t, bucket)

		client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("path/to/deep/file.txt"),
			Body:   stringReader("nested"),
		})

		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("path/to/deep/file.txt"),
		})
		require.NoError(t, err)
		defer out.Body.Close()

		body, _ := io.ReadAll(out.Body)
		assert.Equal(t, "nested", string(body))
	})

	t.Run("List", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.name + "-obj-list"
		tgt.createBkt(t, bucket)

		for _, kv := range []struct{ key, val string }{
			{"docs/a.txt", "a"},
			{"docs/b.txt", "b"},
			{"images/c.png", "c"},
		} {
			client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(kv.key),
				Body:   stringReader(kv.val),
			})
		}

		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)
		assert.Len(t, out.Contents, 3)
	})

	t.Run("ListWithPrefix", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.name + "-obj-prefix"
		tgt.createBkt(t, bucket)

		for _, kv := range []struct{ key, val string }{
			{"docs/a.txt", "a"},
			{"docs/b.txt", "b"},
			{"images/c.png", "c"},
		} {
			client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(kv.key),
				Body:   stringReader(kv.val),
			})
		}

		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
			Prefix: aws.String("docs/"),
		})
		require.NoError(t, err)
		assert.Len(t, out.Contents, 2)
		for _, obj := range out.Contents {
			assert.True(t, strings.HasPrefix(aws.ToString(obj.Key), "docs/"))
		}
	})

	t.Run("Large", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.name + "-obj-large"
		tgt.createBkt(t, bucket)

		data := bytes.Repeat([]byte("X"), 5*1024*1024) // 5MB
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("large.bin"),
			Body:   bytes.NewReader(data),
		})
		require.NoError(t, err)

		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("large.bin"),
		})
		require.NoError(t, err)
		defer out.Body.Close()

		assert.Equal(t, int64(len(data)), aws.ToInt64(out.ContentLength))
		body, _ := io.ReadAll(out.Body)
		assert.Equal(t, data, body)
	})

	t.Run("FormUpload", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.name + "-form-upload"
		tgt.createBkt(t, bucket)

		// Simulate a browser form upload via multipart/form-data POST. The
		// server can accept HTTP before the bucket assignment path is writable,
		// so retry transient 5xx responses at startup.
		var lastStatus int
		var lastErr error
		require.Eventually(t, func() bool {
			var buf bytes.Buffer
			w := multipart.NewWriter(&buf)
			require.NoError(t, w.WriteField("key", "uploaded.txt"))
			require.NoError(t, w.WriteField("Content-Type", "text/plain"))
			require.NoError(t, w.WriteField("success_action_status", "201"))
			writeSignedPostPolicy(t, w, tgt, bucket, "uploaded.txt")

			fw, err := w.CreateFormFile("file", "uploaded.txt")
			require.NoError(t, err)
			_, err = fw.Write([]byte("form upload content"))
			require.NoError(t, err)
			require.NoError(t, w.Close())

			req, _ := http.NewRequest(http.MethodPost, tgt.endpoint(0)+"/"+bucket, &buf)
			req.Header.Set("Content-Type", w.FormDataContentType())

			resp, err := http.DefaultClient.Do(req)
			lastErr = err
			if err != nil {
				return false
			}
			defer resp.Body.Close()
			lastStatus = resp.StatusCode
			return resp.StatusCode == http.StatusCreated
		}, 30*time.Second, 500*time.Millisecond, "form upload status=%d err=%v", lastStatus, lastErr)

		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("uploaded.txt"),
		})
		require.NoError(t, err)
		defer out.Body.Close()

		data, _ := io.ReadAll(out.Body)
		assert.Equal(t, "form upload content", string(data))
	})
}

func writeSignedPostPolicy(t *testing.T, w *multipart.Writer, tgt s3Target, bucket, key string) {
	t.Helper()
	date := time.Now().UTC().Format("20060102")
	credential := tgt.accessKey + "/" + date + "/us-east-1/s3/aws4_request"
	policy := map[string]any{
		"expiration": time.Now().UTC().Add(time.Hour).Format("2006-01-02T15:04:05Z"),
		"conditions": []any{
			map[string]string{"bucket": bucket},
			map[string]string{"key": key},
			map[string]string{"Content-Type": "text/plain"},
			map[string]string{"success_action_status": "201"},
			map[string]string{"X-Amz-Credential": credential},
		},
	}
	raw, err := json.Marshal(policy)
	require.NoError(t, err)
	policyB64 := base64.StdEncoding.EncodeToString(raw)
	require.NoError(t, w.WriteField("policy", policyB64))
	require.NoError(t, w.WriteField("X-Amz-Credential", credential))
	require.NoError(t, w.WriteField("X-Amz-Signature", s3auth.SignPostPolicy(policyB64, tgt.secretKey, date, "us-east-1", "s3")))
}
