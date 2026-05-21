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
	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func stringReader(s string) io.Reader {
	return strings.NewReader(s)
}

var _ = ginkgo.Describe("Objects", ginkgo.Label("bucket"), func() {
	describeObjectContext("SingleNode", func() s3Target {
		return newSingleNodeS3Target()
	})

	describeObjectContext("Cluster4Node", func() s3Target {
		return newSharedClusterS3Target(ginkgo.GinkgoTB())
	})
})

func describeObjectContext(name string, factory func() s3Target) {
	ginkgo.Context(name, func() {
		var tgt s3Target

		ginkgo.BeforeEach(func() {
			tgt = factory()
		})

		runObjectCases(func() s3Target { return tgt })
	})
}

func runObjectCases(getTgt func() s3Target) {
	ginkgo.It("puts and gets an object (PutAndGet)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := tgt.pickNode(0)
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
		ginkgo.DeferCleanup(out.Body.Close)

		body, err := io.ReadAll(out.Body)
		require.NoError(t, err)
		assert.Equal(t, "hello grainfs", string(body))
		assert.Equal(t, "text/plain", aws.ToString(out.ContentType))
		assert.Equal(t, int64(13), aws.ToInt64(out.ContentLength))
	})

	ginkgo.It("heads an object (Head)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := tgt.pickNode(0)
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

	ginkgo.It("returns NotFound when heading a missing object (HeadNotFound)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := tgt.pickNode(0)
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
		assert.Equalf(t, "NotFound", apiErr.ErrorCode(), "head missing object error: %v", err)
	})

	ginkgo.It("deletes an object (Delete)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := tgt.pickNode(0)
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

	ginkgo.It("deletes a nonexistent object idempotently (DeleteNonexistent)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := tgt.pickNode(0)
		ctx := context.Background()
		bucket := tgt.name + "-obj-delnone"
		tgt.createBkt(t, bucket)

		_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("never-existed.txt"),
		})
		require.NoError(t, err)
	})

	ginkgo.It("rejects put to a nonexistent bucket (PutToNonexistentBucket)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := tgt.pickNode(0)
		ctx := context.Background()

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(tgt.name + "-nope-bucket"),
			Key:    aws.String("file.txt"),
			Body:   stringReader("data"),
		})
		require.Error(t, err)
	})

	ginkgo.It("rejects get for a missing object (GetNonexistent)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := tgt.pickNode(0)
		ctx := context.Background()
		bucket := tgt.name + "-obj-getnone"
		tgt.createBkt(t, bucket)

		_, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("nope.txt"),
		})
		require.Error(t, err)
	})

	ginkgo.It("overwrites an object (Overwrite)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := tgt.pickNode(0)
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
		ginkgo.DeferCleanup(out.Body.Close)

		body, _ := io.ReadAll(out.Body)
		assert.Equal(t, "version2", string(body))
		assert.Equal(t, int64(8), aws.ToInt64(out.ContentLength))
	})

	ginkgo.It("handles nested keys (NestedKey)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := tgt.pickNode(0)
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
		ginkgo.DeferCleanup(out.Body.Close)

		body, _ := io.ReadAll(out.Body)
		assert.Equal(t, "nested", string(body))
	})

	ginkgo.It("lists objects (List)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := tgt.pickNode(0)
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

	ginkgo.It("lists objects with a prefix (ListWithPrefix)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := tgt.pickNode(0)
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

	ginkgo.It("round-trips a large object (Large)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := tgt.pickNode(0)
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
		ginkgo.DeferCleanup(out.Body.Close)

		assert.Equal(t, int64(len(data)), aws.ToInt64(out.ContentLength))
		body, _ := io.ReadAll(out.Body)
		assert.Equal(t, data, body)
	})

	ginkgo.It("accepts browser form upload (FormUpload)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := tgt.pickNode(0)
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
		ginkgo.DeferCleanup(out.Body.Close)

		data, _ := io.ReadAll(out.Body)
		assert.Equal(t, "form upload content", string(data))
	})
}

func writeSignedPostPolicy(t testing.TB, w *multipart.Writer, tgt s3Target, bucket, key string) {
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
