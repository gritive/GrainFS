package e2e

import (
	"context"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// TestCacheE2E probes the cache-coherence surface (PUT-overwrite, DELETE,
// HEAD-after-PUT) at the S3 API level. The cache itself is internal; we only
// assert subsequent reads see fresh data. Shared single + shared cluster
// fixtures, each sub-test gets its own bucket via uniqueBucket.
var _ = ginkgo.Describe("Cache", func() {
	describeCacheContext("SingleNode", func() s3Target {
		return newSingleNodeS3Target()
	})
	describeCacheContext("Cluster4Node", func() s3Target {
		return newSharedClusterS3Target(ginkgo.GinkgoTB())
	})
})

func describeCacheContext(name string, factory func() s3Target) {
	ginkgo.Context(name, func() {
		var tgt s3Target

		ginkgo.BeforeEach(func() {
			tgt = factory()
		})

		runCacheCases(func() s3Target { return tgt })
	})
}

func runCacheCases(getTgt func() s3Target) {
	ginkgo.It("reads fresh data after overwrite (ReadConsistency)", func() {
		t := ginkgo.GinkgoTB()
		ctx := context.Background()
		tgt := getTgt()
		cli := tgt.pickNode(0)
		bucket := tgt.uniqueBucket(t, "cacheread")
		key := "cached-key"
		body := "cache-test-data-v1"

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			Body:        strings.NewReader(body),
			ContentType: aws.String("text/plain"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for i := 0; i < 2; i++ {
			resp, err := cli.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			gomega.Expect(string(data)).To(gomega.Equal(body))
		}

		bodyV2 := "cache-test-data-v2"
		_, err = cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			Body:        strings.NewReader(bodyV2),
			ContentType: aws.String("text/plain"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		resp, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		gomega.Expect(string(data)).To(gomega.Equal(bodyV2))
	})

	ginkgo.It("invalidates reads after delete (DeleteInvalidation)", func() {
		t := ginkgo.GinkgoTB()
		ctx := context.Background()
		tgt := getTgt()
		cli := tgt.pickNode(0)
		bucket := tgt.uniqueBucket(t, "cachedel")
		key := "del-key"

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader("to-be-deleted"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		resp, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		resp.Body.Close()

		_, err = cli.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).To(gomega.HaveOccurred())
	})

	ginkgo.It("updates HEAD metadata after PUT overwrite (HeadAfterPut)", func() {
		t := ginkgo.GinkgoTB()
		ctx := context.Background()
		tgt := getTgt()
		cli := tgt.pickNode(0)
		bucket := tgt.uniqueBucket(t, "cachehead")
		key := "head-key"

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader("short"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		head1, err := cli.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(*head1.ContentLength).To(gomega.Equal(int64(5)))

		_, err = cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader("much longer content"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		head2, err := cli.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(*head2.ContentLength).To(gomega.Equal(int64(19)))
	})
}
