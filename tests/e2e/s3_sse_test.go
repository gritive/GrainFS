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

// TestS3SSEE2E exercises the SSE-S3 (AES256) S3 surface (PUT/GET/HEAD/COPY)
// plus the not-implemented branches for SSE-KMS and SSE-C. Shared single +
// shared cluster fixtures, uniqueBucket per sub-test.
func TestS3SSEE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runS3SSECases(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runS3SSECases(t, newSharedClusterS3Target(t))
	})
}

func runS3SSECases(t *testing.T, tgt s3Target) {
	t.Helper()
	ctx := context.Background()
	cli := tgt.pickNode(0)

	t.Run("AES256Roundtrip", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "sseaes")

		putOut, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucket),
			Key:                  aws.String("src.txt"),
			Body:                 strings.NewReader("sse data"),
			ServerSideEncryption: types.ServerSideEncryptionAes256,
		})
		require.NoError(t, err)
		require.Equal(t, types.ServerSideEncryptionAes256, putOut.ServerSideEncryption)

		headOut, err := cli.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("src.txt"),
		})
		require.NoError(t, err)
		require.Equal(t, types.ServerSideEncryptionAes256, headOut.ServerSideEncryption)

		getOut, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("src.txt"),
		})
		require.NoError(t, err)
		_, _ = io.ReadAll(getOut.Body)
		require.NoError(t, getOut.Body.Close())
		require.Equal(t, types.ServerSideEncryptionAes256, getOut.ServerSideEncryption)
	})

	t.Run("CopyPreservesSSE", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "ssecopy")

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucket),
			Key:                  aws.String("src.txt"),
			Body:                 strings.NewReader("sse data"),
			ServerSideEncryption: types.ServerSideEncryptionAes256,
		})
		require.NoError(t, err)

		copyOut, err := cli.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String("copy.txt"),
			CopySource: aws.String(bucket + "/src.txt"),
		})
		require.NoError(t, err)
		require.Equal(t, types.ServerSideEncryptionAes256, copyOut.ServerSideEncryption)

		copyHeadOut, err := cli.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("copy.txt"),
		})
		require.NoError(t, err)
		require.Equal(t, types.ServerSideEncryptionAes256, copyHeadOut.ServerSideEncryption)
	})

	t.Run("SSEKMSNotImplemented", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "ssekms")
		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucket),
			Key:                  aws.String("kms.txt"),
			Body:                 strings.NewReader("kms"),
			ServerSideEncryption: types.ServerSideEncryptionAwsKms,
			SSEKMSKeyId:          aws.String("key-1"),
		})
		requireS3ErrorCode(t, err, "NotImplemented")
	})

	t.Run("SSECNotImplemented", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "ssec")
		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucket),
			Key:                  aws.String("sse-c.txt"),
			Body:                 strings.NewReader("sse-c"),
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String("customer-key"),
		})
		requireS3ErrorCode(t, err, "NotImplemented")
	})
}

func requireS3ErrorCode(t *testing.T, err error, code string) {
	t.Helper()
	require.Error(t, err)
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, code, apiErr.ErrorCode())
}
