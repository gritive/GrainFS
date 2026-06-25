// Latest-version selection by completion time (ModTime-primary) e2e.
//
// Reproduces the bug where a multipart upload CREATED BEFORE a PutObject but
// COMPLETED AFTER it was left non-latest, because latest-version selection used
// max-VersionID (UUIDv7 minted at create) instead of ModTime (stamped at the
// completing write). The multipart object's VersionID is lower (minted first)
// yet its write completed last, so it is the true latest.
//
// Limitation: ModTime is second-granularity, so the create/complete vs. PUT
// times must differ by >= 1s for the rule to apply; the test sleeps >= 1s.
package e2e

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Latest version by completion time", ginkgo.Label("bucket"), func() {
	runLatestVersionByModTimeSpecs()
})

func runLatestVersionByModTimeSpecs() {
	ginkgo.Context("SingleNode", func() {
		var tgt s3Target
		ginkgo.BeforeEach(func() {
			tgt = newSingleNodeS3Target()
		})
		runLatestVersionByModTimeCases(func() s3Target { return tgt })
	})

	ginkgo.Context("Cluster4Node", func() {
		var tgt s3Target
		ginkgo.BeforeEach(func() {
			tgt = newSharedClusterS3Target(ginkgo.GinkgoTB())
		})
		runLatestVersionByModTimeCases(func() s3Target { return tgt })
	})
}

func runLatestVersionByModTimeCases(getTgt func() s3Target) {
	ginkgo.It("treats a multipart completed after a later PUT as latest (CompletedAfterPutWins)", func() {
		t, tgt, client := multipartFixture(getTgt, "lvm-probe")
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "lvm")
		key := "completed-after-put.bin"

		// Versioning enabled so both versions coexist and IsLatest is assertable.
		_, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket: aws.String(bucket),
			VersioningConfiguration: &types.VersioningConfiguration{
				Status: types.BucketVersioningStatusEnabled,
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// 1. CreateMultipartUpload FIRST: the upload's VersionID is minted now
		//    (lowest UUIDv7), but its ModTime is stamped at CompleteMultipartUpload.
		mpuBody := bytes.Repeat([]byte("M"), 5<<20)
		initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		uploadID := initOut.UploadId

		part, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   uploadID,
			PartNumber: aws.Int32(1),
			Body:       bytes.NewReader(mpuBody),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// 2. PutObject AFTER the upload was created: higher VersionID (minted now),
		//    ModTime stamped now.
		putBody := []byte("put-written-in-the-middle")
		putOut, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(putBody),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		putVID := aws.ToString(putOut.VersionId)
		gomega.Expect(putVID).NotTo(gomega.BeEmpty())

		// 3. ModTime is second-granularity: sleep so the multipart-complete ModTime
		//    is strictly greater than the PUT's. This >= 1s gap IS the documented
		//    limitation of the fix.
		time.Sleep(1100 * time.Millisecond)

		// 4. CompleteMultipartUpload LAST: lower VersionID, highest ModTime.
		compOut, err := client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: uploadID,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{
					{PartNumber: aws.Int32(1), ETag: part.ETag},
				},
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		mpuVID := aws.ToString(compOut.VersionId)
		gomega.Expect(mpuVID).NotTo(gomega.BeEmpty())
		// Sanity: the upload's VersionID really is LOWER than the PUT's, so a
		// max-VID rule would (wrongly) pick the PUT.
		gomega.Expect(mpuVID < putVID).To(gomega.BeTrue(),
			"multipart VersionID (minted at create) must be lower than the later PUT's")

		// The last-completed write (the multipart) is the latest: GET returns it.
		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(getOut.Body.Close)
		gotBody, _ := io.ReadAll(getOut.Body)
		gomega.Expect(gotBody).To(gomega.Equal(mpuBody),
			"GET must return the multipart body (last completed write), not the earlier PUT")

		// ListObjectVersions flags the multipart version IsLatest, the PUT not.
		lvOut, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket: aws.String(bucket),
			Prefix: aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		latestByVID := map[string]bool{}
		for _, v := range lvOut.Versions {
			latestByVID[aws.ToString(v.VersionId)] = aws.ToBool(v.IsLatest)
		}
		gomega.Expect(latestByVID).To(gomega.HaveKey(mpuVID))
		gomega.Expect(latestByVID).To(gomega.HaveKey(putVID))
		gomega.Expect(latestByVID[mpuVID]).To(gomega.BeTrue(),
			"the multipart version (completed last) must be IsLatest")
		gomega.Expect(latestByVID[putVID]).To(gomega.BeFalse(),
			"the earlier PUT (completed before the multipart) must NOT be IsLatest")
	})
}
