// AppendObject API e2e (via aws-sdk-go-v2).
//
// Target table-driven: the same case set runs against a single-node fixture
// and a multi-node cluster fixture. aws-sdk-go-v2 already exposes
// `WriteOffsetBytes *int64` on PutObjectInput which serializes to the
// `x-amz-write-offset-bytes` header, so no middleware injection is required.
package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// appendTarget abstracts a fixture (single-node or cluster) under test.
//
// pickNode(i) returns the S3 client bound to node i. For single-node fixtures
// every i collapses to the same client, so common-case tests can call
// pickNode(0) uniformly.
type appendTarget struct {
	name      string
	nodes     int
	pickNode  func(i int) *s3.Client
	createBkt func(t *testing.T, bucket string)
	isCluster bool
}

func TestAppendObjectE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		tgt := newSingleNodeAppendTarget()
		runCommonAppendCases(t, tgt)
	})

	t.Run("Cluster4Node", func(t *testing.T) {
		skipIfShort(t, "4-node cluster boot is too slow for -short")
		tgt := newClusterAppendTarget(t, 4)
		runCommonAppendCases(t, tgt)
		runClusterOnlyAppendCases(t, tgt)
	})
}

// ----- fixtures -----

func newSingleNodeAppendTarget() appendTarget {
	return appendTarget{
		name:  "single",
		nodes: 1,
		pickNode: func(i int) *s3.Client {
			return testS3Client
		},
		createBkt: func(t *testing.T, bucket string) {
			createBucket(t, bucket)
		},
		isCluster: false,
	}
}

func newClusterAppendTarget(t *testing.T, nodes int) appendTarget {
	t.Helper()
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      nodes,
		Mode:       ClusterModeDynamicJoin,
		ClusterKey: "E2E-APPEND-KEY",
		LogPrefix:  "grainfs-append",
		DisableNFS: true,
		DisableNBD: true,
	})

	// Wait for IAM key propagation across all nodes; otherwise non-leader
	// nodes 403 on first request.
	for i := range c.procs {
		iamWaitKeyReady(t, c.httpURLs[i], c.accessKey, c.secretKey, 30*time.Second)
	}

	return appendTarget{
		name:  "cluster4",
		nodes: nodes,
		pickNode: func(i int) *s3.Client {
			return c.S3Client(i % nodes)
		},
		createBkt: func(t *testing.T, bucket string) {
			c.GrantAdminOnBuckets(bucket)
			createBucketWithClient(t, c.S3Client(c.leaderIdx), bucket)
		},
		isCluster: true,
	}
}

// ----- cases (common) -----

func runCommonAppendCases(t *testing.T, tgt appendTarget) {
	bucket := "append-" + tgt.name
	tgt.createBkt(t, bucket)
	client := tgt.pickNode(0)

	t.Run("InitialAppend", func(t *testing.T) {
		key := "obj-init"
		require.NoError(t, putAppend(client, bucket, key, 0, []byte("hello")))
		require.Equal(t, []byte("hello"), getObject(t, client, bucket, key))
	})

	t.Run("SequentialAppends", func(t *testing.T) {
		key := "obj-seq"
		require.NoError(t, putAppend(client, bucket, key, 0, []byte("foo")))
		require.NoError(t, putAppend(client, bucket, key, 3, []byte("bar")))
		require.NoError(t, putAppend(client, bucket, key, 6, []byte("baz")))
		require.Equal(t, []byte("foobarbaz"), getObject(t, client, bucket, key))
	})

	t.Run("OffsetMismatch", func(t *testing.T) {
		key := "obj-mismatch"
		require.NoError(t, putAppend(client, bucket, key, 0, []byte("aaa")))
		err := putAppend(client, bucket, key, 99, []byte("bbb"))
		require.Error(t, err)
		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "InvalidWriteOffset", apiErr.ErrorCode())
	})

	t.Run("PlainPutOverwritesAppendable", func(t *testing.T) {
		key := "obj-overwrite"
		require.NoError(t, putAppend(client, bucket, key, 0, []byte("aaaa")))
		// Plain PUT (no x-amz-write-offset-bytes header) overwrites the
		// appendable object.
		_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte("xx")),
		})
		require.NoError(t, err)
		require.Equal(t, []byte("xx"), getObject(t, client, bucket, key))
	})
}

// ----- cases (cluster-only) -----

func runClusterOnlyAppendCases(t *testing.T, tgt appendTarget) {
	require.True(t, tgt.isCluster, "clusterOnly cases require cluster fixture")
	bucket := "append-" + tgt.name + "-cluster"
	tgt.createBkt(t, bucket)

	t.Run("ConcurrentAppendsFromDifferentNodes", func(t *testing.T) {
		// All N goroutines race for offset 0 from different nodes. Exactly
		// one must win; the rest must surface InvalidWriteOffset. This
		// exercises the cluster forwarding + raft-serialized offset check.
		key := "obj-race"
		var wg sync.WaitGroup
		var successes int64
		var mismatches int64
		for i := 0; i < tgt.nodes; i++ {
			wg.Add(1)
			go func(node int) {
				defer wg.Done()
				cli := tgt.pickNode(node)
				err := putAppend(cli, bucket, key, 0, []byte(fmt.Sprintf("n%d-", node)))
				if err == nil {
					atomic.AddInt64(&successes, 1)
					return
				}
				var apiErr smithy.APIError
				if errors.As(err, &apiErr) && apiErr.ErrorCode() == "InvalidWriteOffset" {
					atomic.AddInt64(&mismatches, 1)
					return
				}
				t.Errorf("node %d: unexpected error: %v", node, err)
			}(i)
		}
		wg.Wait()
		assert.Equal(t, int64(1), atomic.LoadInt64(&successes), "exactly one append must win the offset-0 race")
		assert.Equal(t, int64(tgt.nodes-1), atomic.LoadInt64(&mismatches), "all losers must surface InvalidWriteOffset")
	})

	t.Run("AppendsFromDifferentNodesForwardToOwner", func(t *testing.T) {
		// Serial appends, each issued against a different node. The
		// distributed backend must forward to the owner so the final
		// object reflects every chunk in order. This validates the
		// non-owner -> owner forwarding path end-to-end.
		key := "obj-forward"
		chunks := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma"), []byte("delta")}
		var offset int64
		for i, chunk := range chunks {
			cli := tgt.pickNode(i)
			require.NoError(t, putAppend(cli, bucket, key, offset, chunk),
				"append chunk %d via node %d at offset %d", i, i%tgt.nodes, offset)
			offset += int64(len(chunk))
		}
		// Read back from any node — every replica must converge.
		for i := 0; i < tgt.nodes; i++ {
			body := getObject(t, tgt.pickNode(i), bucket, key)
			assert.Equal(t, []byte("alphabetagammadelta"), body, "node %d view", i)
		}
	})
}

// ----- helpers -----

// putAppend issues a PutObject with WriteOffsetBytes set and returns the raw
// SDK error so callers can inspect APIError codes.
func putAppend(client *s3.Client, bucket, key string, offset int64, body []byte) error {
	off := offset
	_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:           aws.String(bucket),
		Key:              aws.String(key),
		Body:             bytes.NewReader(body),
		WriteOffsetBytes: &off,
	})
	return err
}

func getObject(t *testing.T, client *s3.Client, bucket, key string) []byte {
	t.Helper()
	resp, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return data
}
