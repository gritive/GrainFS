//go:build compat

package compat

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func TestMixedClusterRolling(t *testing.T) {
	prev := prevBinary(t)
	cur := getBinary()

	// node 0 = prev binary (acting as seed/leader)
	// node 1 = current binary (follower, rolling-upgraded)
	c := startCompatCluster(t, []string{prev, cur})
	t.Cleanup(func() { c.Stop() })

	bucket := "mixed-cluster-test"
	_, err := c.S3Client(0).CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)

	wantBody := "mixed cluster data"
	_, err = c.S3Client(0).PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("test-obj"),
		Body:   strings.NewReader(wantBody),
	})
	require.NoError(t, err)

	// Read from the upgraded node
	res, err := c.S3Client(1).GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("test-obj"),
	})
	require.NoError(t, err)
	defer res.Body.Close()
	got, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, wantBody, string(got))
}
