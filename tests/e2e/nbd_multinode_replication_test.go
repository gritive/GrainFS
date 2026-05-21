package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/require"
)

var _ = ginkgo.Describe("NBD multinode replication", func() {
	ginkgo.Context("Cluster3Node", func() {
		var c *e2eCluster

		ginkgo.BeforeEach(func() {
			c = startE2ECluster(ginkgo.GinkgoTB(), e2eClusterOptions{
				Nodes:      3,
				Mode:       ClusterModeStaticPeers,
				DisableNFS: true,
			})
		})

		// Writes a block via NBD on node 0, reads it back, then verifies a
		// non-writer node observes the committed volume object metadata.
		ginkgo.It("routes byte-level writes through cluster metadata", func() {
			t := ginkgo.GinkgoTB()

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			ginkgo.DeferCleanup(cancel)
			c.GrantAdminOnBuckets("__grainfs_volumes")
			ensureE2ENBDVolume(t, ctx, c, "default", 4*1024*1024)

			client := dialE2ENBD(t, fmt.Sprintf("127.0.0.1:%d", c.nbdPorts[0]), "default")
			ginkgo.DeferCleanup(client.Close)

			body := []byte("nbd-byte-level-replication-payload")
			client.WriteAt(t, 0, body)
			client.Flush(t)
			requireNBDReadEventually(t, client, 0, body)

			out, err := c.S3Client(1).ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket: aws.String("__grainfs_volumes"),
				Prefix: aws.String("__vol/default/"),
			})
			require.NoError(t, err)
			require.NotEmpty(t, out.Contents)
		})
	})
})
