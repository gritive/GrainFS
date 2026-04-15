package e2e

import (
	"bytes"
	"context"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func stringReader(s string) io.Reader {
	return strings.NewReader(s)
}

func TestObjects_PutAndGet(t *testing.T) {
	ctx := context.Background()
	createBucket(t, "obj-putget")

	_, err := testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String("obj-putget"),
		Key:         aws.String("hello.txt"),
		Body:        stringReader("hello grainfs"),
		ContentType: aws.String("text/plain"),
	})
	require.NoError(t, err)

	out, err := testS3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("obj-putget"),
		Key:    aws.String("hello.txt"),
	})
	require.NoError(t, err)
	defer out.Body.Close()

	body, err := io.ReadAll(out.Body)
	require.NoError(t, err)
	assert.Equal(t, "hello grainfs", string(body))
	assert.Equal(t, "text/plain", aws.ToString(out.ContentType))
	assert.Equal(t, int64(13), aws.ToInt64(out.ContentLength))
}

func TestObjects_Head(t *testing.T) {
	ctx := context.Background()
	createBucket(t, "obj-head")

	testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("obj-head"),
		Key:    aws.String("meta.txt"),
		Body:   stringReader("metadata test"),
	})

	out, err := testS3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("obj-head"),
		Key:    aws.String("meta.txt"),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(13), aws.ToInt64(out.ContentLength))
	assert.NotEmpty(t, aws.ToString(out.ETag))
}

func TestObjects_HeadNotFound(t *testing.T) {
	ctx := context.Background()
	createBucket(t, "obj-headnf")

	_, err := testS3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("obj-headnf"),
		Key:    aws.String("nope.txt"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "NotFound", apiErr.ErrorCode())
}

func TestObjects_Delete(t *testing.T) {
	ctx := context.Background()
	createBucket(t, "obj-del")

	testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("obj-del"),
		Key:    aws.String("to-delete.txt"),
		Body:   stringReader("gone"),
	})

	_, err := testS3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String("obj-del"),
		Key:    aws.String("to-delete.txt"),
	})
	require.NoError(t, err)

	_, err = testS3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("obj-del"),
		Key:    aws.String("to-delete.txt"),
	})
	require.Error(t, err)
}

func TestObjects_DeleteNonexistent(t *testing.T) {
	ctx := context.Background()
	createBucket(t, "obj-delnone")

	_, err := testS3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String("obj-delnone"),
		Key:    aws.String("never-existed.txt"),
	})
	require.NoError(t, err)
}

func TestObjects_PutToNonexistentBucket(t *testing.T) {
	ctx := context.Background()

	_, err := testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("nope-bucket"),
		Key:    aws.String("file.txt"),
		Body:   stringReader("data"),
	})
	require.Error(t, err)
}

func TestObjects_GetNonexistent(t *testing.T) {
	ctx := context.Background()
	createBucket(t, "obj-getnone")

	_, err := testS3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("obj-getnone"),
		Key:    aws.String("nope.txt"),
	})
	require.Error(t, err)
}

func TestObjects_Overwrite(t *testing.T) {
	ctx := context.Background()
	createBucket(t, "obj-overwrite")

	testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("obj-overwrite"),
		Key:    aws.String("file.txt"),
		Body:   stringReader("v1"),
	})
	testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("obj-overwrite"),
		Key:    aws.String("file.txt"),
		Body:   stringReader("version2"),
	})

	out, err := testS3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("obj-overwrite"),
		Key:    aws.String("file.txt"),
	})
	require.NoError(t, err)
	defer out.Body.Close()

	body, _ := io.ReadAll(out.Body)
	assert.Equal(t, "version2", string(body))
	assert.Equal(t, int64(8), aws.ToInt64(out.ContentLength))
}

func TestObjects_NestedKey(t *testing.T) {
	ctx := context.Background()
	createBucket(t, "obj-nested")

	testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("obj-nested"),
		Key:    aws.String("path/to/deep/file.txt"),
		Body:   stringReader("nested"),
	})

	out, err := testS3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("obj-nested"),
		Key:    aws.String("path/to/deep/file.txt"),
	})
	require.NoError(t, err)
	defer out.Body.Close()

	body, _ := io.ReadAll(out.Body)
	assert.Equal(t, "nested", string(body))
}

func TestObjects_List(t *testing.T) {
	ctx := context.Background()
	createBucket(t, "obj-list")

	for _, kv := range []struct{ key, val string }{
		{"docs/a.txt", "a"},
		{"docs/b.txt", "b"},
		{"images/c.png", "c"},
	} {
		testS3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("obj-list"),
			Key:    aws.String(kv.key),
			Body:   stringReader(kv.val),
		})
	}

	out, err := testS3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("obj-list"),
	})
	require.NoError(t, err)
	assert.Len(t, out.Contents, 3)
}

func TestObjects_ListWithPrefix(t *testing.T) {
	ctx := context.Background()
	createBucket(t, "obj-prefix")

	for _, kv := range []struct{ key, val string }{
		{"docs/a.txt", "a"},
		{"docs/b.txt", "b"},
		{"images/c.png", "c"},
	} {
		testS3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("obj-prefix"),
			Key:    aws.String(kv.key),
			Body:   stringReader(kv.val),
		})
	}

	out, err := testS3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("obj-prefix"),
		Prefix: aws.String("docs/"),
	})
	require.NoError(t, err)
	assert.Len(t, out.Contents, 2)
	for _, obj := range out.Contents {
		assert.True(t, strings.HasPrefix(aws.ToString(obj.Key), "docs/"))
	}
}

func TestObjects_Large(t *testing.T) {
	ctx := context.Background()
	createBucket(t, "obj-large")

	data := bytes.Repeat([]byte("X"), 5*1024*1024) // 5MB
	_, err := testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("obj-large"),
		Key:    aws.String("large.bin"),
		Body:   bytes.NewReader(data),
	})
	require.NoError(t, err)

	out, err := testS3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("obj-large"),
		Key:    aws.String("large.bin"),
	})
	require.NoError(t, err)
	defer out.Body.Close()

	assert.Equal(t, int64(len(data)), aws.ToInt64(out.ContentLength))
	body, _ := io.ReadAll(out.Body)
	assert.Equal(t, data, body)
}

func TestE2E_FormUpload(t *testing.T) {
	createBucket(t, "form-upload")

	// Simulate a browser form upload via multipart/form-data POST
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	w.WriteField("key", "uploaded.txt")
	w.WriteField("Content-Type", "text/plain")
	w.WriteField("success_action_status", "201")

	fw, err := w.CreateFormFile("file", "uploaded.txt")
	require.NoError(t, err)
	fw.Write([]byte("form upload content"))
	w.Close()

	req, _ := http.NewRequest(http.MethodPost, testServerURL+"/form-upload", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, 201, resp.StatusCode)

	// Verify the object was stored
	ctx := context.Background()
	out, err := testS3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("form-upload"),
		Key:    aws.String("uploaded.txt"),
	})
	require.NoError(t, err)
	defer out.Body.Close()

	data, _ := io.ReadAll(out.Body)
	assert.Equal(t, "form upload content", string(data))
}
