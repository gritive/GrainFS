package e2e

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func TestE2E_JoinedNodeEdgeForwardsBeforeDataReady(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      2,
		Mode:       ClusterModeDynamicJoin,
		ClusterKey: "E2E-DYNAMIC-JOIN-KEY",
		AccessKey:  "join-ak",
		SecretKey:  "join-sk",
		LogPrefix:  "grainfs-dynamic-join",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	joinClient := c.S3Client(1)
	seedClient := c.S3Client(0)
	bucket := "dynamic-join-edge"
	key := "hello.txt"
	body := []byte("hello dynamic join")

	require.Eventually(t, func() bool {
		return tryCreateBucket(ctx, joinClient, bucket) == nil
	}, 30*time.Second, 500*time.Millisecond)
	require.NoError(t, tryPutObject(ctx, joinClient, bucket, key, body))

	gotSeed, err := getObjectBytes(ctx, seedClient, bucket, key)
	require.NoError(t, err)
	require.Equal(t, body, gotSeed)
	gotJoin, err := getObjectBytes(ctx, joinClient, bucket, key)
	require.NoError(t, err)
	require.Equal(t, body, gotJoin)

	_, err = joinClient.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
}

func TestE2E_AllServicesAvailableOnJoinedNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      2,
		Mode:       ClusterModeDynamicJoin,
		ClusterKey: "E2E-DYNAMIC-JOIN-KEY",
		AccessKey:  "join-ak",
		SecretKey:  "join-sk",
		LogPrefix:  "grainfs-dynamic-join",
	})
	waitForPortsParallel(t, c.nfs4Ports, 30*time.Second)
	waitForPortsParallel(t, c.nbdPorts, 30*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 5 * time.Second}

	reqBody := []byte(`{"namespace":["join_ns"],"properties":{"owner":"e2e"}}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.httpURLs[1]+"/iceberg/v1/namespaces", bytes.NewReader(reqBody))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	req, err = http.NewRequestWithContext(ctx, http.MethodGet, c.httpURLs[0]+"/iceberg/v1/namespaces", nil)
	require.NoError(t, err)
	resp, err = client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, string(data), "join_ns")
}

func TestE2E_DefaultBucketOnlySeedCreates(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      2,
		Mode:       ClusterModeDynamicJoin,
		ClusterKey: "E2E-DYNAMIC-JOIN-KEY",
		AccessKey:  "join-ak",
		SecretKey:  "join-sk",
		LogPrefix:  "grainfs-dynamic-join",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i, endpoint := range c.httpURLs {
		client := c.S3Client(i)
		out, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
		require.NoError(t, err)
		var defaults int
		for _, bucket := range out.Buckets {
			if aws.ToString(bucket.Name) == "default" {
				defaults++
			}
		}
		require.Equal(t, 1, defaults, "default bucket should exist once as shared cluster metadata at %s", endpoint)
	}
}
