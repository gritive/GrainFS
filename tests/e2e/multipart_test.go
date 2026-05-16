package e2e

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultipart_Complete(t *testing.T) {
	ctx := context.Background()
	createBucket(t, "mp-complete")

	key := "multipart-file.bin"
	part1Data := bytes.Repeat([]byte("A"), 1024)
	part2Data := bytes.Repeat([]byte("B"), 512)

	// Initiate
	initOut, err := testS3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:      aws.String("mp-complete"),
		Key:         aws.String(key),
		ContentType: aws.String("application/octet-stream"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(initOut.UploadId))

	uploadID := initOut.UploadId

	// Upload parts
	p1, err := testS3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("mp-complete"),
		Key:        aws.String(key),
		UploadId:   uploadID,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(part1Data),
	})
	require.NoError(t, err)

	p2, err := testS3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("mp-complete"),
		Key:        aws.String(key),
		UploadId:   uploadID,
		PartNumber: aws.Int32(2),
		Body:       bytes.NewReader(part2Data),
	})
	require.NoError(t, err)

	// Complete
	_, err = testS3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String("mp-complete"),
		Key:      aws.String(key),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: p1.ETag},
				{PartNumber: aws.Int32(2), ETag: p2.ETag},
			},
		},
	})
	require.NoError(t, err)

	// Verify assembled object
	getOut, err := testS3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("mp-complete"),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	defer getOut.Body.Close()

	body, _ := io.ReadAll(getOut.Body)
	expected := append(part1Data, part2Data...)
	assert.Equal(t, expected, body)
	assert.Equal(t, int64(len(expected)), aws.ToInt64(getOut.ContentLength))
}

func TestMultipart_Abort(t *testing.T) {
	ctx := context.Background()
	createBucket(t, "mp-abort")

	initOut, err := testS3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String("mp-abort"),
		Key:    aws.String("aborted.bin"),
	})
	require.NoError(t, err)

	testS3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("mp-abort"),
		Key:        aws.String("aborted.bin"),
		UploadId:   initOut.UploadId,
		PartNumber: aws.Int32(1),
		Body:       stringReader("data"),
	})

	_, err = testS3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String("mp-abort"),
		Key:      aws.String("aborted.bin"),
		UploadId: initOut.UploadId,
	})
	require.NoError(t, err)

	// Object should not exist
	_, err = testS3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("mp-abort"),
		Key:    aws.String("aborted.bin"),
	})
	assert.Error(t, err)
}

func TestMultipart_List(t *testing.T) {
	ctx := context.Background()
	createBucket(t, "mp-list")

	exerciseMultipartListingFeature(t, ctx, testS3Client, "mp-list", "part", false)
}

type multipartListingFixture struct {
	Bucket  string
	Key     string
	Prefix  string
	Upload  string
	ETagOne string
	ETagTwo string
}

func exerciseMultipartListingFeature(t testing.TB, ctx context.Context, client *s3.Client, bucket, partLabel string, waitForParts bool) multipartListingFixture {
	t.Helper()

	fixture := createIncompleteMultipartListingFixture(t, ctx, client, bucket, partLabel)
	assertMultipartListingFeature(t, ctx, client, fixture, waitForParts)
	return fixture
}

func createIncompleteMultipartListingFixture(t testing.TB, ctx context.Context, client *s3.Client, bucket, partLabel string) multipartListingFixture {
	t.Helper()

	fixture := multipartListingFixture{
		Bucket: bucket,
		Key:    "listed/incomplete.bin",
		Prefix: "listed/",
	}
	initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(fixture.Bucket),
		Key:         aws.String(fixture.Key),
		ContentType: aws.String("application/octet-stream"),
	})
	require.NoError(t, err)
	fixture.Upload = aws.ToString(initOut.UploadId)
	require.NotEmpty(t, fixture.Upload)
	t.Cleanup(func() {
		_, _ = client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(fixture.Bucket),
			Key:      aws.String(fixture.Key),
			UploadId: aws.String(fixture.Upload),
		})
	})

	p1, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(fixture.Bucket),
		Key:        aws.String(fixture.Key),
		UploadId:   aws.String(fixture.Upload),
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader([]byte(partLabel + "-one")),
	})
	require.NoError(t, err)
	p2, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(fixture.Bucket),
		Key:        aws.String(fixture.Key),
		UploadId:   aws.String(fixture.Upload),
		PartNumber: aws.Int32(2),
		Body:       bytes.NewReader([]byte(partLabel + "-two")),
	})
	require.NoError(t, err)
	fixture.ETagOne = aws.ToString(p1.ETag)
	fixture.ETagTwo = aws.ToString(p2.ETag)
	return fixture
}

func assertMultipartListingFeature(t testing.TB, ctx context.Context, client *s3.Client, fixture multipartListingFixture, waitForParts bool) {
	t.Helper()

	uploads, err := client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(fixture.Bucket),
		Prefix: aws.String(fixture.Prefix),
	})
	require.NoError(t, err)
	require.Len(t, uploads.Uploads, 1)
	assert.Equal(t, fixture.Key, aws.ToString(uploads.Uploads[0].Key))
	assert.Equal(t, fixture.Upload, aws.ToString(uploads.Uploads[0].UploadId))

	var parts *s3.ListPartsOutput
	listParts := func() error {
		var err error
		parts, err = client.ListParts(ctx, &s3.ListPartsInput{
			Bucket:   aws.String(fixture.Bucket),
			Key:      aws.String(fixture.Key),
			UploadId: aws.String(fixture.Upload),
		})
		return err
	}
	if waitForParts {
		require.Eventually(t, func() bool { return listParts() == nil }, 30*time.Second, 500*time.Millisecond)
	} else {
		require.NoError(t, listParts())
	}
	require.Len(t, parts.Parts, 2)
	assert.Equal(t, int32(1), aws.ToInt32(parts.Parts[0].PartNumber))
	assert.Equal(t, fixture.ETagOne, aws.ToString(parts.Parts[0].ETag))
	assert.Equal(t, int32(2), aws.ToInt32(parts.Parts[1].PartNumber))
	assert.Equal(t, fixture.ETagTwo, aws.ToString(parts.Parts[1].ETag))
}

func TestMultipart_ThreeParts(t *testing.T) {
	ctx := context.Background()
	createBucket(t, "mp-three")

	key := "three-parts.bin"
	partsData := [][]byte{
		bytes.Repeat([]byte("X"), 256),
		bytes.Repeat([]byte("Y"), 512),
		bytes.Repeat([]byte("Z"), 128),
	}

	initOut, err := testS3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String("mp-three"),
		Key:    aws.String(key),
	})
	require.NoError(t, err)

	completedParts := make([]types.CompletedPart, len(partsData))
	for i, data := range partsData {
		pOut, err := testS3Client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String("mp-three"),
			Key:        aws.String(key),
			UploadId:   initOut.UploadId,
			PartNumber: aws.Int32(int32(i + 1)),
			Body:       bytes.NewReader(data),
		})
		require.NoError(t, err)
		completedParts[i] = types.CompletedPart{
			PartNumber: aws.Int32(int32(i + 1)),
			ETag:       pOut.ETag,
		}
	}

	_, err = testS3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String("mp-three"),
		Key:      aws.String(key),
		UploadId: initOut.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	require.NoError(t, err)

	getOut, err := testS3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("mp-three"),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	defer getOut.Body.Close()

	body, _ := io.ReadAll(getOut.Body)
	var expected []byte
	for _, d := range partsData {
		expected = append(expected, d...)
	}
	assert.Equal(t, expected, body)
}
