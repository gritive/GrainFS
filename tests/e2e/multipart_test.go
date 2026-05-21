// Multipart upload S3 API e2e (target table-driven).
//
// The same case set runs against a single-node fixture and a 4-node cluster
// fixture. Listing helpers (exerciseMultipartListingFeature et al.) are
// retained because cluster_test.go reuses them.
package e2e

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Multipart uploads", func() {
	runMultipartSpecs()
	runMultipartChunkedUploadPartSpecs()
	runMultipartGetPartNumberSpecs()
})

func runMultipartSpecs() {
	ginkgo.Context("Multipart SingleNode", func() {
		var tgt s3Target
		ginkgo.BeforeEach(func() {
			tgt = newSingleNodeS3Target()
		})
		runMultipartCases(func() s3Target { return tgt })
	})

	ginkgo.Context("Multipart Cluster4Node", func() {
		var tgt s3Target
		ginkgo.BeforeEach(func() {
			tgt = newSharedClusterS3Target(ginkgo.GinkgoTB())
		})
		runMultipartCases(func() s3Target { return tgt })
	})
}

func multipartFixture(getTgt func() s3Target, probePrefix string) (testing.TB, s3Target, *s3.Client) {
	t := ginkgo.GinkgoTB()
	tgt := getTgt()
	client := tgt.pickNode(0)

	// Cluster fixtures reject CreateMultipartUpload until the
	// `multipart_listing_v1` capability has finished its rolling-upgrade
	// handshake. Gate once on a probe bucket so per-case CreateMultipartUpload
	// calls don't race the upgrade. Single-node fixtures don't need this
	// gate; if the shared server ever returns 503 here it indicates a
	// pre-existing failure in unrelated tests.
	if tgt.isCluster {
		probe := tgt.uniqueBucket(t, probePrefix)
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		ginkgo.DeferCleanup(cancel)
		waitForMultipartListingCreate(t, ctx, client, probe, multipartListingKey, 120*time.Second)
	}
	return t, tgt, client
}

func runMultipartCases(getTgt func() s3Target) {
	ginkgo.It("completes a multipart upload", func() {
		t, tgt, client := multipartFixture(getTgt, "mp-probe")
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "mp-complete")

		key := "multipart-file.bin"
		part1Data := bytes.Repeat([]byte("A"), 5<<20)
		part2Data := bytes.Repeat([]byte("B"), 512)

		initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			ContentType: aws.String("application/octet-stream"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(aws.ToString(initOut.UploadId)).NotTo(gomega.BeEmpty())

		uploadID := initOut.UploadId

		p1, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   uploadID,
			PartNumber: aws.Int32(1),
			Body:       bytes.NewReader(part1Data),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		p2, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   uploadID,
			PartNumber: aws.Int32(2),
			Body:       bytes.NewReader(part2Data),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: uploadID,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{
					{PartNumber: aws.Int32(1), ETag: p1.ETag},
					{PartNumber: aws.Int32(2), ETag: p2.ETag},
				},
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(getOut.Body.Close)

		body, _ := io.ReadAll(getOut.Body)
		expected := append(part1Data, part2Data...)
		gomega.Expect(body).To(gomega.Equal(expected))
		gomega.Expect(aws.ToInt64(getOut.ContentLength)).To(gomega.Equal(int64(len(expected))))
	})

	ginkgo.It("aborts a multipart upload", func() {
		t, tgt, client := multipartFixture(getTgt, "mp-probe")
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "mp-abort")

		initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("aborted.bin"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String("aborted.bin"),
			UploadId:   initOut.UploadId,
			PartNumber: aws.Int32(1),
			Body:       stringReader("data"),
		})

		_, err = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String("aborted.bin"),
			UploadId: initOut.UploadId,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("aborted.bin"),
		})
		gomega.Expect(err).To(gomega.HaveOccurred())
	})

	ginkgo.It("lists incomplete multipart uploads and parts", func() {
		t, tgt, client := multipartFixture(getTgt, "mp-probe")
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "mp-list")

		exerciseMultipartListingFeature(t, ctx, client, bucket, "part", tgt.isCluster)
	})

	ginkgo.It("completes a three-part upload", func() {
		t, tgt, client := multipartFixture(getTgt, "mp-probe")
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "mp-three")

		key := "three-parts.bin"
		partsData := [][]byte{
			bytes.Repeat([]byte("X"), 5<<20),
			bytes.Repeat([]byte("Y"), 5<<20),
			bytes.Repeat([]byte("Z"), 128),
		}

		initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		completedParts := make([]types.CompletedPart, len(partsData))
		for i, data := range partsData {
			pOut, err := client.UploadPart(ctx, &s3.UploadPartInput{
				Bucket:     aws.String(bucket),
				Key:        aws.String(key),
				UploadId:   initOut.UploadId,
				PartNumber: aws.Int32(int32(i + 1)),
				Body:       bytes.NewReader(data),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			completedParts[i] = types.CompletedPart{
				PartNumber: aws.Int32(int32(i + 1)),
				ETag:       pOut.ETag,
			}
		}

		_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: initOut.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: completedParts,
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(getOut.Body.Close)

		body, _ := io.ReadAll(getOut.Body)
		var expected []byte
		for _, d := range partsData {
			expected = append(expected, d...)
		}
		gomega.Expect(body).To(gomega.Equal(expected))
	})
}

// ----- listing helpers (also used by cluster_test.go) -----

type multipartListingFixture struct {
	Bucket  string
	Key     string
	Prefix  string
	Upload  string
	ETagOne string
	ETagTwo string
}

const multipartListingKey = "listed/incomplete.bin"

func exerciseMultipartListingFeature(t testing.TB, ctx context.Context, client *s3.Client, bucket, partLabel string, waitForParts bool) multipartListingFixture {
	t.Helper()

	fixture := createIncompleteMultipartListingFixture(t, ctx, client, bucket, partLabel)
	assertMultipartListingFeature(t, ctx, client, fixture, waitForParts)
	return fixture
}

func createIncompleteMultipartListingFixture(t testing.TB, ctx context.Context, client *s3.Client, bucket, partLabel string) multipartListingFixture {
	t.Helper()

	fixture := multipartListingFixture{
		Bucket: bucket,
		Key:    multipartListingKey,
		Prefix: "listed/",
	}
	initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(fixture.Bucket),
		Key:         aws.String(fixture.Key),
		ContentType: aws.String("application/octet-stream"),
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	fixture.Upload = aws.ToString(initOut.UploadId)
	gomega.Expect(fixture.Upload).NotTo(gomega.BeEmpty())
	t.Cleanup(func() {
		_, _ = client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(fixture.Bucket),
			Key:      aws.String(fixture.Key),
			UploadId: aws.String(fixture.Upload),
		})
	})

	p1, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(fixture.Bucket),
		Key:        aws.String(fixture.Key),
		UploadId:   aws.String(fixture.Upload),
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader([]byte(partLabel + "-one")),
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	p2, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(fixture.Bucket),
		Key:        aws.String(fixture.Key),
		UploadId:   aws.String(fixture.Upload),
		PartNumber: aws.Int32(2),
		Body:       bytes.NewReader([]byte(partLabel + "-two")),
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	fixture.ETagOne = aws.ToString(p1.ETag)
	fixture.ETagTwo = aws.ToString(p2.ETag)
	return fixture
}

func waitForMultipartListingCreate(t testing.TB, ctx context.Context, client *s3.Client, bucket, key string, timeout time.Duration) {
	t.Helper()

	var lastErr error
	gomega.Eventually(func() bool {
		initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			lastErr = err
			return false
		}
		_, err = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: initOut.UploadId,
		})
		if err != nil {
			lastErr = err
			return false
		}
		return true
	}).WithTimeout(timeout).WithPolling(time.Second).
		Should(gomega.BeTrue(), "multipart listing create gate did not open: %v", lastErr)
}

func assertMultipartListingFeature(t testing.TB, ctx context.Context, client *s3.Client, fixture multipartListingFixture, waitForParts bool) {
	t.Helper()

	var uploads *s3.ListMultipartUploadsOutput
	listUploads := func() error {
		var err error
		uploads, err = client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
			Bucket: aws.String(fixture.Bucket),
			Prefix: aws.String(fixture.Prefix),
		})
		return err
	}
	if waitForParts {
		gomega.Eventually(func() bool {
			return listUploads() == nil && len(uploads.Uploads) == 1
		}).WithTimeout(30 * time.Second).WithPolling(500 * time.Millisecond).Should(gomega.BeTrue())
	} else {
		gomega.Expect(listUploads()).To(gomega.Succeed())
	}
	gomega.Expect(uploads.Uploads).To(gomega.HaveLen(1))
	gomega.Expect(aws.ToString(uploads.Uploads[0].Key)).To(gomega.Equal(fixture.Key))
	gomega.Expect(aws.ToString(uploads.Uploads[0].UploadId)).To(gomega.Equal(fixture.Upload))

	var parts *s3.ListPartsOutput
	listParts := func() error {
		var err error
		parts, err = client.ListParts(ctx, &s3.ListPartsInput{
			Bucket:   aws.String(fixture.Bucket),
			Key:      aws.String(fixture.Key),
			UploadId: aws.String(fixture.Upload),
		})
		return err
	}
	if waitForParts {
		gomega.Eventually(func() bool {
			return listParts() == nil && multipartListingPartsMatch(parts, fixture)
		}).WithTimeout(30 * time.Second).WithPolling(500 * time.Millisecond).Should(gomega.BeTrue())
	} else {
		gomega.Expect(listParts()).To(gomega.Succeed())
	}
	assertMultipartListingParts(t, parts, fixture)
}

func multipartListingPartsMatch(parts *s3.ListPartsOutput, fixture multipartListingFixture) bool {
	if parts == nil || len(parts.Parts) != 2 {
		return false
	}
	return aws.ToInt32(parts.Parts[0].PartNumber) == 1 &&
		aws.ToString(parts.Parts[0].ETag) == fixture.ETagOne &&
		aws.ToInt32(parts.Parts[1].PartNumber) == 2 &&
		aws.ToString(parts.Parts[1].ETag) == fixture.ETagTwo
}

func assertMultipartListingParts(t testing.TB, parts *s3.ListPartsOutput, fixture multipartListingFixture) {
	t.Helper()

	gomega.Expect(parts.Parts).To(gomega.HaveLen(2))
	gomega.Expect(aws.ToInt32(parts.Parts[0].PartNumber)).To(gomega.Equal(int32(1)))
	gomega.Expect(aws.ToString(parts.Parts[0].ETag)).To(gomega.Equal(fixture.ETagOne))
	gomega.Expect(aws.ToInt32(parts.Parts[1].PartNumber)).To(gomega.Equal(int32(2)))
	gomega.Expect(aws.ToString(parts.Parts[1].ETag)).To(gomega.Equal(fixture.ETagTwo))
}
