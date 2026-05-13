package e2e

// Slice 3 of refactor/unify-storage-paths: end-to-end verification that the
// cluster-mode scrubber auto-repairs a shard deleted from disk by pulling
// surviving shards from peers and reconstructing via Reed-Solomon.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_ClusterScrubber_AutoRepair runs a 3-node 2+1 cluster, puts one
// object, kills an on-disk shard on one node, and asserts the scrubber
// restores it from peer survivors within a few scrub cycles. A GET after
// repair must return byte-identical content.
//
// We deliberately choose 2+1 on 3 nodes rather than 3+2 on 5 because
// 5-node Raft bootstrap on loopback is flaky in CI; 3 nodes converge
// quickly and still exercise the full repair code path (read survivors
// from peers, reconstruct, write back locally).
func TestE2E_ClusterScrubber_AutoRepair(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-node e2e in -short mode")
	}

	const (
		bucketName = "sc-bucket"
		keyName    = "sc-obj"
		numNodes   = 3
	)

	// Scrub interval kept tight so the test doesn't wait minutes for a
	// cycle. ShardPlacementMonitor piggybacks on this interval.
	cluster := startE2ECluster(t, e2eClusterOptions{
		Nodes:         numNodes,
		Mode:          ClusterModeDynamicJoin,
		LogPrefix:     "grainfs-scrubber",
		ScrubInterval: "2s",
	})
	cluster.GrantAdminOnBuckets(bucketName)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	endpoints := append([]string(nil), cluster.httpURLs...)

	leaderIdx, err := waitForWritableEndpoint(
		ctx,
		endpoints,
		120*time.Second,
		5*time.Second,
		1*time.Second,
		func(attemptCtx context.Context, endpoint string) error {
			c := ecS3Client(endpoint, cluster.accessKey, cluster.secretKey)
			return tryCreateBucket(attemptCtx, c, bucketName)
		},
	)
	require.NoError(t, err, "no leader ready")
	client := cluster.S3Client(leaderIdx)

	// PUT a random object large enough to exercise all 5 shards.
	payload := make([]byte, 256*1024)
	_, err = rand.Read(payload)
	require.NoError(t, err)
	sum := sha256.Sum256(payload)

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(keyName),
		Body:   bytes.NewReader(payload),
	})
	require.NoError(t, err)

	// Locate a node that actually holds shard 0 on disk. We don't assume
	// placement order and we don't know the versionID (UUIDv7) ahead of
	// time — walk the object's shards subtree per node until we find one.
	var victimNode int
	var victimShard string
	for i := 0; i < numNodes; i++ {
		root := filepath.Join(cluster.dataDirs[i], "shards", bucketName, keyName)
		_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, _ error) error {
			if d == nil || d.IsDir() {
				return nil
			}
			if filepath.Base(p) == "shard_0" {
				victimNode = i
				victimShard = p
				return filepath.SkipAll
			}
			return nil
		})
		if victimShard != "" {
			break
		}
	}
	require.NotEmpty(t, victimShard, "expected at least one node to hold shard_0 on disk")

	// Kill shard 0 on the victim node. The scrubber on that node should
	// detect the missing local shard, call RepairShard, pull the other 4
	// shards from peers, reconstruct shard 0, and rewrite it to disk.
	require.NoError(t, os.Remove(victimShard))
	t.Logf("deleted shard 0 at %s (node %d)", victimShard, victimNode)

	// Wait up to 30s (15 scrub cycles at 2s) for the shard to be restored.
	require.Eventually(t, func() bool {
		info, err := os.Stat(victimShard)
		return err == nil && info.Size() > 0
	}, 30*time.Second, 500*time.Millisecond, "scrubber did not restore shard 0")

	// GET must still round-trip byte-identical.
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(keyName),
	})
	require.NoError(t, err)
	defer out.Body.Close()
	var gotBuf bytes.Buffer
	_, err = gotBuf.ReadFrom(out.Body)
	require.NoError(t, err)
	assert.Equal(t, sum, sha256.Sum256(gotBuf.Bytes()), "content must match after auto-repair")
}
