// append_composite_etag_post_coalesce_test.go — regression-lock for the
// composite ETag returned by AppendObject AFTER a coalesce has consumed the
// object's raw Segments.
//
// Gap: append_off_raft_regression_test.go locks the composite ETag only for
// the NON-coalesce case (N small appends, no coalesce trigger). A coalesce
// CONSUMES Segments into a Coalesced ref, so a post-coalesce append that
// recomputed the ETag from base.Segments would DROP every pre-coalesce
// per-call digest and return md5(new)-1 instead of md5(c1..cN+1)-(N+1). This
// test drives enough appends to trigger a coalesce, waits for the coalesce to
// publish, then appends once more and asserts the FINAL APPEND RESPONSE ETag
// is the S3 composite of ALL N+1 per-call MD5s.
//
// The assertion uses the synchronous append RESPONSE ETag (not a GET) to avoid
// the known single-node post-coalesce read gap; this runs on the shared 4-node
// cluster fixture only.
package e2e

import (
	"bytes"
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("append composite ETag survives coalesce", func() {
	var tgt s3Target

	ginkgo.BeforeEach(func() {
		tgt = newSharedClusterS3Target(ginkgo.GinkgoTB())
	})

	// Append N segments past the coalesce trigger, wait for the coalesce to
	// publish, then append once more and assert the FINAL APPEND RESPONSE ETag
	// equals the S3 composite of all N+1 per-call MD5s.
	ginkgo.It("final append ETag is composite of all per-call MD5s after coalesce", func() {
		t := ginkgo.GinkgoTB()
		bucket := tgt.uniqueBucket(t, "append-etag-post-coalesce")
		client := tgt.pickNode(0)
		ctx := context.Background()
		key := "obj-etag-post-coalesce"

		coalesceMetricBaseline := metricCounterTotal(t, tgt, `grainfs_append_coalesce_total{result="success"}`)

		// Same trigger profile as runCoalesceCase: 20 chunks > the coalesce
		// segment-count trigger (16). Each chunk's bytes are distinct so each
		// per-call MD5 is distinct.
		const chunkSize = 8 * 1024
		const numChunks = 20
		bodies := make([][]byte, 0, numChunks+1)
		var off int64
		for i := 0; i < numChunks; i++ {
			chunk := make([]byte, chunkSize)
			for j := range chunk {
				chunk[j] = byte(i + 1)
			}
			gomega.Expect(putAppend(client, bucket, key, off, chunk)).To(gomega.Succeed(), "chunk %d", i)
			bodies = append(bodies, chunk)
			off += int64(len(chunk))
		}

		// Wait for the coalesce to publish: the success counter must advance past
		// the baseline on at least one node (same convergence signal as
		// runCoalesceCase).
		gomega.Eventually(func() bool {
			return metricCounterTotal(t, tgt, `grainfs_append_coalesce_total{result="success"}`) > coalesceMetricBaseline
		}, 15*time.Second, 200*time.Millisecond).Should(gomega.BeTrue(),
			"coalesce must publish before the post-coalesce append")

		// One more append AFTER the coalesce consumed Segments. The synchronous
		// response ETag must be the composite of ALL per-call MD5s, not just the
		// surviving (post-coalesce) Segments.
		finalChunk := make([]byte, chunkSize)
		for j := range finalChunk {
			finalChunk[j] = 0xAB
		}
		finalOff := off
		out, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:           aws.String(bucket),
			Key:              aws.String(key),
			Body:             bytes.NewReader(finalChunk),
			WriteOffsetBytes: &finalOff,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "post-coalesce append must succeed")
		bodies = append(bodies, finalChunk)

		expectedETag := compositeETagFor(bodies...)
		gotETag := stripETagQuotes(aws.ToString(out.ETag))
		gomega.Expect(gotETag).To(gomega.Equal(expectedETag),
			"post-coalesce append ETag must be composite of ALL %d per-call MD5s", len(bodies))
	})
})
