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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
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

func runAppendObject(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		tgt := newSingleNodeS3Target()
		runCommonAppendCases(t, tgt)
	})

	t.Run("Cluster4Node", func(t *testing.T) {
		// Dedicated (non-shared) — runClusterOnlyAppendCases contains
		// OwnerKillSurvives which kills + restarts a node; running that on
		// the shared fixture would temporarily degrade subsequent tests
		// reading from sharedCluster. Split off once that case is moved
		// to its own file.
		tgt := newClusterS3Target(t, 4)
		runCommonAppendCases(t, tgt)
		runClusterOnlyAppendCases(t, tgt)
	})
}

// ----- cases (common) -----

func runCommonAppendCases(t *testing.T, tgt s3Target) {
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

// findOwnerForSingleGroup returns the index of the segment owner node
// under the single-group default configuration. The cluster harness
// currently has no API to query per-group data-Raft leaders, so this
// helper assumes the single-group invariant and returns the meta-Raft
// leaderIdx (which coincides under that invariant).
//
// When multi-group support lands, add a NEW helper:
//
//	findOwnerForGroup(c *e2eCluster, group string) int
//
// that issues a real data-Raft leader query (admin API). Leave this
// function name in place but deprecated — callers must migrate before
// removing it. The naming forces the migration to be deliberate, not
// silent.
//
// Design source: 2026-05-19-appendobject-hardening-design.md § Follow-up 1.
func findOwnerForSingleGroup(c *e2eCluster) int {
	if c == nil {
		return -1
	}
	return c.leaderIdx
}

// ----- cases (cluster-only) -----

func runClusterOnlyAppendCases(t *testing.T, tgt s3Target) {
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

	t.Run("OwnerKillSurvives", func(t *testing.T) {
		require.True(t, tgt.isCluster)
		c := tgt.cluster
		require.NotNil(t, c)

		ownerBucket := "append-owner-kill-" + tgt.name
		tgt.createBkt(t, ownerBucket)
		key := "obj-survive"

		// Drive 16 appends to trigger coalesce → obj.Coalesced should have
		// at least 1 entry. The owner is the data-Raft leader.
		const chunkSize = 1024
		var off int64
		var expected []byte
		for i := 0; i < 16; i++ {
			chunk := bytes.Repeat([]byte{byte(i + 1)}, chunkSize)
			require.NoError(t, putAppend(tgt.pickNode(0), ownerBucket, key, off, chunk))
			off += int64(len(chunk))
			expected = append(expected, chunk...)
		}
		// Wait for coalesce to land (Metrics endpoint reports success).
		require.Eventually(t, func() bool {
			for i := 0; i < tgt.nodes; i++ {
				if metricCounterAtLeast(t, tgt, i, `grainfs_append_coalesce_total{result="success"}`, 1) {
					return true
				}
			}
			return false
		}, 10*time.Second, 200*time.Millisecond)

		// Identify the data-Raft leader (Task 23) and kill it.
		ownerIdx := findOwnerForSingleGroup(c)
		require.GreaterOrEqual(t, ownerIdx, 0)
		killedNodeID := c.nodeID(ownerIdx)

		c.KillNode(ownerIdx)
		// CRITICAL: defer (not t.Cleanup) — sibling sub-tests must see the
		// fully restored cluster, not an N-1 cluster.
		defer c.RestartNode(t, ownerIdx)

		// Poll the cluster status on a surviving peer until a new leader is
		// elected (not the killed node). This is the direct evidence that
		// leader rotation completed.
		//
		// Note: AwaitWriteFromNonOwner is not usable here because the 4-node
		// cluster uses EC 2+2 which requires all 4 shards for writes; with the
		// owner dead only 3 nodes are available so new writes fail with
		// ServiceUnavailable. Status-poll is the correct rotation signal when
		// EC stripe width equals cluster size.
		surviving := (ownerIdx + 1) % tgt.nodes
		require.Eventually(t, func() bool {
			resp, err := http.Get(c.httpURLs[surviving] + "/api/cluster/status")
			if err != nil {
				return false
			}
			defer resp.Body.Close()
			var s map[string]any
			if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
				return false
			}
			leader, _ := s["leader_id"].(string)
			return leader != "" && leader != killedNodeID
		}, 60*time.Second, 500*time.Millisecond, "no new leader elected within 60s")

		// GET from a surviving non-owner peer; EC reconstruct must yield the
		// full body bytes. With k=2 parity=2, we only need k=2 shards; the
		// 3 surviving nodes satisfy that constraint.
		body := getObject(t, tgt.pickNode(surviving), ownerBucket, key)
		require.Equal(t, expected, body)
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

// TestAppendObjectsE2E groups all S3 AppendObject scenarios under one entry.
func TestAppendObjectsE2E(t *testing.T) {
	t.Run("AppendObject", runAppendObject)
	t.Run("Coalesce", runAppendCoalesce)
	t.Run("MidSizeBody", runAppendMidSizeBody)
	t.Run("ForwardBufferSaturation", runAppendForwardBufferSaturation)
	t.Run("SizeCap", runAppendSizeCap)
}
