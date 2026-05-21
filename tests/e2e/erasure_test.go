package e2e

import (
	"bytes"
	"context"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ = ginkgo.Describe("EC objects", ginkgo.Label("bucket"), func() {
	describeECObjectsContext("SingleNode", func() s3Target {
		return newSingleNodeS3Target()
	})

	describeECObjectsContext("Cluster4Node", func() s3Target {
		return newSharedClusterS3Target(ginkgo.GinkgoTB())
	})
})

func describeECObjectsContext(name string, factory func() s3Target) {
	ginkgo.Context(name, func() {
		var (
			ctx context.Context
			tgt s3Target
			cli *s3.Client
		)

		ginkgo.BeforeEach(func() {
			t := ginkgo.GinkgoTB()
			ctx = context.Background()
			tgt = factory()
			cli = tgt.pickNode(0)

			if tgt.isCluster {
				probe := tgt.name + "-ec-mp-probe"
				tgt.createBkt(t, probe)
				gateCtx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
				ginkgo.DeferCleanup(cancel)
				waitForMultipartListingCreate(t, gateCtx, cli, probe, multipartListingKey, 120*time.Second)
			}
		})

		runECObjectsCases(func() context.Context { return ctx }, func() s3Target { return tgt }, func() *s3.Client { return cli })
	})
}

func runECObjectsCases(getCtx func() context.Context, getTgt func() s3Target, getClient func() *s3.Client) {
	ginkgo.It("puts and gets basic objects (BasicPutGet)", func() {
		t := ginkgo.GinkgoTB()
		ctx := getCtx()
		tgt := getTgt()
		cli := getClient()
		bucket := tgt.uniqueBucket(t, "basic")
		cases := []struct {
			name    string
			key     string
			content string
		}{
			{"small_object", "small.txt", "hello erasure coding"},
			{"medium_object", "medium.txt", strings.Repeat("EC test data ", 1000)},
			{"nested_key", "path/to/deep/file.txt", "nested EC content"},
		}
		for _, tc := range cases {
			_, err := cli.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(tc.key),
				Body:   strings.NewReader(tc.content),
			})
			require.NoError(t, err, tc.name)

			getOut, err := cli.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(tc.key),
			})
			require.NoError(t, err, tc.name)
			ginkgo.DeferCleanup(getOut.Body.Close)

			body, _ := io.ReadAll(getOut.Body)
			assert.Equal(t, tc.content, string(body), tc.name)
			assert.Equal(t, int64(len(tc.content)), aws.ToInt64(getOut.ContentLength), tc.name)
		}
	})

	ginkgo.It("round-trips a large object (LargeObject)", func() {
		t := ginkgo.GinkgoTB()
		ctx := getCtx()
		tgt := getTgt()
		cli := getClient()
		bucket := tgt.uniqueBucket(t, "large")
		// 5MiB body — exceeds the default shard size, forcing a true EC stripe.
		data := bytes.Repeat([]byte("X"), 5*1024*1024)
		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("large.bin"),
			Body:   bytes.NewReader(data),
		})
		require.NoError(t, err)

		getOut, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("large.bin"),
		})
		require.NoError(t, err)
		ginkgo.DeferCleanup(getOut.Body.Close)

		body, _ := io.ReadAll(getOut.Body)
		assert.Equal(t, data, body)
	})

	ginkgo.It("completes multipart upload (MultipartUpload)", func() {
		t := ginkgo.GinkgoTB()
		ctx := getCtx()
		tgt := getTgt()
		cli := getClient()
		bucket := tgt.uniqueBucket(t, "multipart")
		key := "multipart-ec.bin"
		part1Data := bytes.Repeat([]byte("A"), 5*1024*1024)
		part2Data := bytes.Repeat([]byte("B"), 512)

		initOut, err := cli.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)

		p1, err := cli.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   initOut.UploadId,
			PartNumber: aws.Int32(1),
			Body:       bytes.NewReader(part1Data),
		})
		require.NoError(t, err)

		p2, err := cli.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   initOut.UploadId,
			PartNumber: aws.Int32(2),
			Body:       bytes.NewReader(part2Data),
		})
		require.NoError(t, err)

		_, err = cli.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: initOut.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{
					{PartNumber: aws.Int32(1), ETag: p1.ETag},
					{PartNumber: aws.Int32(2), ETag: p2.ETag},
				},
			},
		})
		require.NoError(t, err)

		getOut, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		ginkgo.DeferCleanup(getOut.Body.Close)

		body, _ := io.ReadAll(getOut.Body)
		expected := append(part1Data, part2Data...)
		assert.Equal(t, expected, body)
	})

	ginkgo.It("exposes bucket operations (BucketOperations)", func() {
		t := ginkgo.GinkgoTB()
		ctx := getCtx()
		tgt := getTgt()
		cli := getClient()
		bucket := tgt.uniqueBucket(t, "bktops")

		_, err := cli.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)

		listOut, err := cli.ListBuckets(ctx, &s3.ListBucketsInput{})
		require.NoError(t, err)
		found := false
		for _, b := range listOut.Buckets {
			if aws.ToString(b.Name) == bucket {
				found = true
				break
			}
		}
		assert.True(t, found, "newly created bucket %s missing from ListBuckets", bucket)

		// Decision #8 keeps bucket deletion on the admin socket; data-plane
		// bucket coverage here is HeadBucket/ListBuckets visibility.
	})

	ginkgo.It("deletes and overwrites objects (DeleteAndOverwrite)", func() {
		t := ginkgo.GinkgoTB()
		ctx := getCtx()
		tgt := getTgt()
		cli := getClient()
		bucket := tgt.uniqueBucket(t, "delover")

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("file.txt"),
			Body:   strings.NewReader("v1"),
		})
		require.NoError(t, err)

		_, err = cli.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("file.txt"),
		})
		require.NoError(t, err)

		_, err = cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("file.txt"),
		})
		assert.Error(t, err)

		_, err = cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("over.txt"),
			Body:   strings.NewReader("version1"),
		})
		require.NoError(t, err)

		_, err = cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("over.txt"),
			Body:   strings.NewReader("version2"),
		})
		require.NoError(t, err)

		getOut, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("over.txt"),
		})
		require.NoError(t, err)
		ginkgo.DeferCleanup(getOut.Body.Close)

		body, _ := io.ReadAll(getOut.Body)
		assert.Equal(t, "version2", string(body))
	})
}
