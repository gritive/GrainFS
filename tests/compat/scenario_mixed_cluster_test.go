//go:build compat

package compat

import (
	"context"
	"io"
	"net/http"
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

func TestMixedClusterRejectsMigrationCutoverUntilAllNodesCapable(t *testing.T) {
	prev := prevBinary(t)
	cur := getBinary()
	c := startCompatCluster(t, []string{prev, cur, cur})
	t.Cleanup(func() { c.Stop() })

	body := []byte(`{"bucket":"compat-cutover","upstream_url":"http://127.0.0.1:9000","access_key":"ak","secret_key":"sk"}`)
	status, resp := putCompatAdminJSON(t, c.AdminSock(1), "/v1/upstreams", body)
	require.Equal(t, http.StatusNoContent, status, resp)

	status, resp = postCompatAdminJSON(t, c.AdminSock(1), "/v1/migration/cutover", []byte(`{"bucket":"compat-cutover"}`))
	require.Equal(t, http.StatusConflict, status, resp)
	require.Contains(t, resp, "migration_cutover_v1")
}
