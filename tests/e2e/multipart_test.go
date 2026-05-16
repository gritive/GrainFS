package e2e

import (
	"bytes"
	"context"
	"io"
	"testing"

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

	key := "listed/incomplete.bin"
	initOut, err := testS3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:      aws.String("mp-list"),
		Key:         aws.String(key),
		ContentType: aws.String("application/octet-stream"),
	})
	require.NoError(t, err)
	uploadID := aws.ToString(initOut.UploadId)
	require.NotEmpty(t, uploadID)
	t.Cleanup(func() {
		_, _ = testS3Client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
			Bucket:   aws.String("mp-list"),
			Key:      aws.String(key),
			UploadId: aws.String(uploadID),
		})
	})

	p1, err := testS3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("mp-list"),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader([]byte("part-one")),
	})
	require.NoError(t, err)
	p2, err := testS3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("mp-list"),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(2),
		Body:       bytes.NewReader([]byte("part-two")),
	})
	require.NoError(t, err)

	uploads, err := testS3Client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String("mp-list"),
		Prefix: aws.String("listed/"),
	})
	require.NoError(t, err)
	require.Len(t, uploads.Uploads, 1)
	assert.Equal(t, key, aws.ToString(uploads.Uploads[0].Key))
	assert.Equal(t, uploadID, aws.ToString(uploads.Uploads[0].UploadId))

	parts, err := testS3Client.ListParts(ctx, &s3.ListPartsInput{
		Bucket:   aws.String("mp-list"),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	require.NoError(t, err)
	require.Len(t, parts.Parts, 2)
	assert.Equal(t, int32(1), aws.ToInt32(parts.Parts[0].PartNumber))
	assert.Equal(t, aws.ToString(p1.ETag), aws.ToString(parts.Parts[0].ETag))
	assert.Equal(t, int32(2), aws.ToInt32(parts.Parts[1].PartNumber))
	assert.Equal(t, aws.ToString(p2.ETag), aws.ToString(parts.Parts[1].ETag))
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
