package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Cluster join", ginkgo.Label("bucket"), func() {
	ginkgo.Context("default bucket", func() {
		ginkgo.It("creates the default bucket only on the seed", func() {
			t := ginkgo.GinkgoTB()
			c := startE2ECluster(t, e2eClusterOptions{
				Nodes:      2,
				Mode:       ClusterModeDynamicJoin,
				ClusterKey: "E2E-DYNAMIC-JOIN-KEY",
				AccessKey:  "join-ak",
				SecretKey:  "join-sk",
				LogPrefix:  "grainfs-dynamic-join",
			})
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			ginkgo.DeferCleanup(cancel)

			for i, endpoint := range c.httpURLs {
				client := c.S3Client(i)
				out, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				var defaults int
				for _, bucket := range out.Buckets {
					if aws.ToString(bucket.Name) == "default" {
						defaults++
					}
				}
				gomega.Expect(defaults).To(gomega.Equal(1), "default bucket should exist once as shared cluster metadata at %s", endpoint)
			}
		})
	})

	ginkgo.Context("services", func() {
		ginkgo.It("forwards joined-node edge writes before data is locally ready", func() {
			runClusterJoinedNodeEdgeForwardsBeforeDataReady(ginkgo.GinkgoTB())
		})
	})
})

func runClusterJoinedNodeEdgeForwardsBeforeDataReady(t testing.TB) {
	t.Helper()
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      2,
		Mode:       ClusterModeDynamicJoin,
		ClusterKey: "E2E-DYNAMIC-JOIN-KEY",
		AccessKey:  "join-ak",
		SecretKey:  "join-sk",
		LogPrefix:  "grainfs-dynamic-join",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	ginkgo.DeferCleanup(cancel)

	joinClient := c.S3Client(1)
	seedClient := c.S3Client(0)
	bucket := "dynamic-join-edge"
	key := "hello.txt"
	body := []byte("hello dynamic join")

	_, err := c.EnsureBucketWritable(ctx, bucket, 60*time.Second)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(tryPutObject(ctx, joinClient, bucket, key, body)).To(gomega.Succeed())

	gotSeed, err := getObjectBytes(ctx, seedClient, bucket, key)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(gotSeed).To(gomega.Equal(body))
	gotJoin, err := getObjectBytes(ctx, joinClient, bucket, key)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(gotJoin).To(gomega.Equal(body))

	_, err = joinClient.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}
