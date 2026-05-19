// AppendObject + Phase B3 coalesce end-to-end.
//
// Runs the same case set against single-node and a 4-node cluster: enough
// appends to trigger coalesce, observe convergence across every node, range-
// read across the coalesced/raw boundary, and assert the
// grainfs_append_coalesce_total{result="success"} counter ticks. Single is
// the degenerate-EC control (k=1, no cross-node distribute); cluster adds
// the EC distribute aspect to the same case.
//
// SingleNode currently fails (intentionally): post-coalesce appendable read
// goes through PartialIO/ReadAt on the storage stack, but single-node
// LocalBackend does not implement PartialIO — the wal wrapper surfaces
// `wal: inner backend does not support ReadAt` as EOF on the post-coalesce
// GET. Tracked in TODOS.md → AppendObject Follow-Ups → "Single-node
// LocalBackend missing PartialIO". The failing subtest is the regression
// signal that unblocks closing the gap.
package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// TestAppendCoalesceE2E is the Phase B3 omnibus e2e for AppendObject coalesce.
// AppendObject N=20 segments → wait for coalesce → range read → metrics
// endpoint reports grainfs_append_coalesce_total{result="success"} >= 1.
//
// Owner-kill scenarios are exercised by the unit-level coverage in
// internal/cluster/coalesce_owner_failure_test.go; this e2e focuses on the
// happy-path observable behavior (round-trip + metrics) on both deployment
// shapes.
func TestAppendCoalesceE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runCoalesceCase(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runCoalesceCase(t, newSharedClusterS3Target(t))
	})
}

// runCoalesceCase drives N appends through tgt, waits for coalesce, and
// asserts the metric ticks on at least one node. Loops over tgt.nodes for
// per-node convergence and metric scraping — single has nodes=1, cluster
// has nodes=4.
func runCoalesceCase(t *testing.T, tgt s3Target) {
	bucket := tgt.uniqueBucket(t, "coalesce")
	client := tgt.pickNode(0)
	key := "obj-coalesce"

	const chunkSize = 8 * 1024
	const numChunks = 20
	var off int64
	var expected []byte
	for i := 0; i < numChunks; i++ {
		chunk := bytes.Repeat([]byte{byte(i + 1)}, chunkSize)
		require.NoError(t, putAppend(client, bucket, key, off, chunk), "chunk %d", i)
		off += int64(len(chunk))
		expected = append(expected, chunk...)
	}

	// Full-body round-trip from any node.
	require.Equal(t, expected, getObject(t, client, bucket, key))

	// Wait for coalesce. The trigger fires when >= COALESCE_SEGMENT_COUNT (16)
	// segments accumulate; with numChunks=20 we should observe a non-empty
	// Coalesced[] within the worker's processing window. HeadObject is not
	// directly available via S3 SDK so we probe by observing that subsequent
	// GETs from any node remain intact (forward-on-read or EC reconstruct,
	// both work).
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		// Read from every node — convergence proves coalesce + EC distribute
		// landed (or, pre-coalesce, that forward-on-read works).
		ok := true
		for i := 0; i < tgt.nodes; i++ {
			body := getObject(t, tgt.pickNode(i), bucket, key)
			if !bytes.Equal(body, expected) {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Range read across what would be the coalesced/raw boundary. Without a
	// HeadObject hook to read internal Coalesced metadata, target the middle
	// of the body — coalesce reduces 16 of 20 raw segments, so the boundary
	// is around offset 16 * 8KiB = 131072.
	rangeStart := int64(16*chunkSize - 1024)
	rangeEnd := int64(16*chunkSize + 1024) // [start, end) inclusive end exclusive
	rangeBody, err := getObjectRange(client, bucket, key, rangeStart, rangeEnd-1)
	require.NoError(t, err)
	require.Equal(t, expected[rangeStart:rangeEnd], rangeBody, "range [%d,%d) mismatch", rangeStart, rangeEnd)

	// Metrics endpoint: at least one coalesce success across all nodes. We
	// inspect each node's /metrics; the OWNER node observed the success.
	require.Eventually(t, func() bool {
		for i := 0; i < tgt.nodes; i++ {
			if metricCounterAtLeast(t, tgt, i, `grainfs_append_coalesce_total{result="success"}`, 1) {
				return true
			}
		}
		return false
	}, 10*time.Second, 200*time.Millisecond, "no node reported a coalesce success")
}

// getObjectRange issues a Range GET (inclusive end per HTTP semantics).
func getObjectRange(client *s3.Client, bucket, key string, startInclusive, endInclusive int64) ([]byte, error) {
	rng := fmt.Sprintf("bytes=%d-%d", startInclusive, endInclusive)
	resp, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(rng),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// metricCounterAtLeast scrapes the target node's /metrics endpoint and
// returns true when the named counter (with labels) is >= threshold. Works
// for both single (1 endpoint) and cluster (N endpoints) targets via
// tgt.endpoint(i).
// Lightweight parser — accepts the prometheus text format line "name{labels} value".
func metricCounterAtLeast(t *testing.T, tgt s3Target, nodeIdx int, metricLine string, threshold float64) bool {
	t.Helper()
	url := tgt.endpoint(nodeIdx) + "/metrics"
	resp, err := http.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "#") || !strings.HasPrefix(line, metricLine) {
			continue
		}
		// Expected format: "<metricLine> <value>"
		var f float64
		if _, err := fmt.Sscanf(line, metricLine+" %f", &f); err == nil {
			if f >= threshold {
				return true
			}
		}
	}
	return false
}
