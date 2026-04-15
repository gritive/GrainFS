package e2e

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestDefaultBucket_ExistsOnStartup(t *testing.T) {
	ctx := context.Background()

	// The "default" bucket should exist immediately after server startup
	out, err := testS3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		t.Fatalf("list buckets: %v", err)
	}

	found := false
	for _, b := range out.Buckets {
		if aws.ToString(b.Name) == "default" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected 'default' bucket to exist on startup, but it was not found")
	}

	// Verify we can use the default bucket immediately
	_, err = testS3Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String("default"),
	})
	if err != nil {
		t.Fatalf("head default bucket: %v", err)
	}
}
