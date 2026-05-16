package e2e

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

func TestS3SSE(t *testing.T) {
	ctx := context.Background()
	bucket := "s3-sse"
	createBucket(t, bucket)

	putOut, err := testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:               aws.String(bucket),
		Key:                  aws.String("src.txt"),
		Body:                 strings.NewReader("sse data"),
		ServerSideEncryption: types.ServerSideEncryptionAes256,
	})
	require.NoError(t, err)
	require.Equal(t, types.ServerSideEncryptionAes256, putOut.ServerSideEncryption)

	headOut, err := testS3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("src.txt"),
	})
	require.NoError(t, err)
	require.Equal(t, types.ServerSideEncryptionAes256, headOut.ServerSideEncryption)

	getOut, err := testS3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("src.txt"),
	})
	require.NoError(t, err)
	_, _ = io.ReadAll(getOut.Body)
	require.NoError(t, getOut.Body.Close())
	require.Equal(t, types.ServerSideEncryptionAes256, getOut.ServerSideEncryption)

	copyOut, err := testS3Client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String("copy.txt"),
		CopySource: aws.String(bucket + "/src.txt"),
	})
	require.NoError(t, err)
	require.Equal(t, types.ServerSideEncryptionAes256, copyOut.ServerSideEncryption)

	copyHeadOut, err := testS3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("copy.txt"),
	})
	require.NoError(t, err)
	require.Equal(t, types.ServerSideEncryptionAes256, copyHeadOut.ServerSideEncryption)

	_, err = testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:               aws.String(bucket),
		Key:                  aws.String("kms.txt"),
		Body:                 strings.NewReader("kms"),
		ServerSideEncryption: types.ServerSideEncryptionAwsKms,
		SSEKMSKeyId:          aws.String("key-1"),
	})
	requireS3ErrorCode(t, err, "NotImplemented")

	_, err = testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:               aws.String(bucket),
		Key:                  aws.String("sse-c.txt"),
		Body:                 strings.NewReader("sse-c"),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String("customer-key"),
	})
	requireS3ErrorCode(t, err, "NotImplemented")
}

func requireS3ErrorCode(t *testing.T, err error, code string) {
	t.Helper()
	require.Error(t, err)
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, code, apiErr.ErrorCode())
}
