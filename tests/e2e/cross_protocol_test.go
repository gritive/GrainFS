package e2e

import (
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestCrossProtocolS3PutNFSStat verifies that S3 PUT invalidates NFS stat cache.
// This is a TDD test: written first, implementation to follow.
func TestCrossProtocolS3PutNFSStat(t *testing.T) {
	ctx := context.Background()
	bucket := "test-cross-protocol-s3-put-nfs"

	// Create bucket via S3
	_, err := testS3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	// Upload file via S3
	key := "test-file.txt"
	content := "hello, world"
	_, err = testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte(content)),
	})
	if err != nil {
		t.Fatalf("s3 put object: %v", err)
	}

	// TODO: Mount NFS and stat the file
	// For now, this test will fail because we haven't implemented:
	// 1. NFS client mounting in tests
	// 2. VFS cache invalidation via Raft OnApply callback
	// 3. Mapping between S3 bucket/key and VFS file paths

	t.Skip("NFS client not yet integrated into E2E tests")

	// Expected behavior after implementation:
	// 1. Mount NFS from testNFSPort
	// 2. Stat the file via NFS
	// 3. Verify file exists and has correct size
	// 4. Verify stat completes within Raft latency + 100ms
}
