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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Cluster scrubber", func() {
	ginkgo.Context("Cluster3Node", func() {
		var cluster *e2eCluster

		ginkgo.BeforeEach(func() {
			const (
				numNodes = 3
			)

			// Scrub interval kept tight so the test doesn't wait minutes for a
			// cycle. ShardPlacementMonitor piggybacks on this interval.
			cluster = startE2ECluster(ginkgo.GinkgoTB(), e2eClusterOptions{
				Nodes:         numNodes,
				Mode:          ClusterModeDynamicJoin,
				LogPrefix:     "grainfs-scrubber",
				ScrubInterval: "2s",
			})
		})

		// Runs a 3-node 2+1 cluster, puts one object, kills an on-disk shard
		// on one node, and asserts the scrubber restores it.
		ginkgo.It("auto-repairs a deleted on-disk shard", func() {
			t := ginkgo.GinkgoTB()

			const (
				bucketName = "sc-bucket"
				keyName    = "sc-obj"
				numNodes   = 3
			)

			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
			ginkgo.DeferCleanup(cancel)

			leaderIdx, err := cluster.EnsureBucketWritable(ctx, bucketName, 120*time.Second)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "no leader ready")
			client := cluster.S3Client(leaderIdx)

			// PUT a random object large enough to exercise all 5 shards.
			payload := make([]byte, 256*1024)
			_, err = rand.Read(payload)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			sum := sha256.Sum256(payload)

			_, err = client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(keyName),
				Body:   bytes.NewReader(payload),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
			gomega.Expect(victimShard).NotTo(gomega.BeEmpty(), "expected at least one node to hold shard_0 on disk")

			// Kill shard 0 on the victim node. The scrubber on that node should
			// detect the missing local shard, call RepairShard, pull the other 4
			// shards from peers, reconstruct shard 0, and rewrite it to disk.
			gomega.Expect(os.Remove(victimShard)).To(gomega.Succeed())
			t.Logf("deleted shard 0 at %s (node %d)", victimShard, victimNode)

			// Wait up to 30s (15 scrub cycles at 2s) for the shard to be restored.
			gomega.Eventually(func() bool {
				info, err := os.Stat(victimShard)
				return err == nil && info.Size() > 0
			}, 30*time.Second, 500*time.Millisecond).Should(gomega.BeTrue(), "scrubber did not restore shard 0")

			// GET must still round-trip byte-identical.
			out, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(keyName),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.DeferCleanup(out.Body.Close)
			var gotBuf bytes.Buffer
			_, err = gotBuf.ReadFrom(out.Body)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(sha256.Sum256(gotBuf.Bytes())).To(gomega.Equal(sum), "content must match after auto-repair")
		})
	})
})
