// append_off_raft_regression_test.go — regression-lock for external S3 behavior
// of appendable and coalesced objects, written before the append/coalesce metadata
// moves off the raft FSM (Slice 1).
//
// Existing tests already lock:
//   - Body concatenation (append_object_test.go / runCommonAppendCases
//     "performs sequential appends")
//   - Coalesce preserves full body + range read across segment boundary
//     (append_coalesce_e2e_test.go / runCoalesceCase)
//
// This file adds the observable contract gaps not yet asserted:
//  1. ETag after N appends is the S3-multipart composite of per-call MD5s.
//  2. HeadObject.ContentLength equals the sum of all append bodies.
//  3. ListObjectsV2 reports the appendable key with the correct total size.
//  4. DeleteObject removes the object; subsequent GetObject → NoSuchKey.
//
// All four scenarios run against a 4-node cluster using the shared cluster
// fixture (newSharedClusterS3Target) because the shared fixture keeps suite
// runtime reasonable; none of these cases mutate cluster topology.
package e2e

import (
	"context"
	"crypto/md5" //nolint:gosec // MD5 is the S3-mandated ETag hash; no security use.
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// compositeETagFor computes the S3-multipart-style composite ETag for a
// sequence of raw bodies: md5(concat(md5_per_call)) + "-<N>".
// Mirrors storage.CompositeETag without importing the internal package.
func compositeETagFor(bodies ...[]byte) string {
	h := md5.New() //nolint:gosec
	for _, b := range bodies {
		seg := md5.Sum(b) //nolint:gosec
		h.Write(seg[:])
	}
	return fmt.Sprintf("%s-%d", hex.EncodeToString(h.Sum(nil)), len(bodies))
}

var _ = ginkgo.Describe("append off-raft regression-lock", func() {
	// Each It obtains its own bucket from the shared 4-node cluster fixture.
	var tgt s3Target

	ginkgo.BeforeEach(func() {
		tgt = newSharedClusterS3Target(ginkgo.GinkgoTB())
	})

	// ── Scenario 1 ──────────────────────────────────────────────────────────
	// Create via AppendObject (offset 0) then 3 more appends.
	// Assert: HeadObject size == sum, GetObject body == concatenation,
	// ETag == composite of per-call MD5s.
	ginkgo.It("appends concatenate and ETag is composite of per-call MD5s", func() {
		t := ginkgo.GinkgoTB()
		bucket := tgt.uniqueBucket(t, "append-reglock-etag")
		client := tgt.pickNode(0)
		ctx := context.Background()

		chunks := [][]byte{
			[]byte("alpha"),
			[]byte("beta"),
			[]byte("gamma"),
			[]byte("delta"),
		}
		key := "obj-etag"
		var off int64
		for _, chunk := range chunks {
			gomega.Expect(putAppend(client, bucket, key, off, chunk)).To(gomega.Succeed())
			off += int64(len(chunk))
		}
		expectedSize := off
		expectedBody := []byte("alphabetagammadelta")
		expectedETag := compositeETagFor(chunks...)

		// HeadObject: size must equal sum of all append payloads.
		head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(aws.ToInt64(head.ContentLength)).To(gomega.Equal(expectedSize),
			"HeadObject.ContentLength must equal sum of append bodies")

		// GetObject: body must be the in-order concatenation.
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		body, err := io.ReadAll(resp.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_ = resp.Body.Close()
		gomega.Expect(body).To(gomega.Equal(expectedBody), "GetObject body must be concat of append chunks in order")

		// ETag (from GetObject response) must be the S3 composite.
		gotETag := aws.ToString(resp.ETag)
		// Strip surrounding quotes if present (AWS SDK sometimes returns "\"etag\"").
		gotETag = stripETagQuotes(gotETag)
		gomega.Expect(gotETag).To(gomega.Equal(expectedETag),
			"ETag must be composite of per-call MD5s: md5(concat(md5_per_call))-N")
	})

	// ── Scenario 2 ──────────────────────────────────────────────────────────
	// Range GET spanning a segment boundary.
	// Appends 4 small segments; range-reads a byte range that straddles the
	// boundary between segments 1 and 2.
	//
	// NOTE: append_coalesce_e2e_test.go / runCoalesceCase already tests range
	// read across the coalesced/raw boundary with 20 segments. This case adds
	// a simpler targeted lock: a range spanning the raw-segment boundary
	// (pre-coalesce) with known exact bytes at the boundary, using a cluster
	// fixture.
	ginkgo.It("range read spans segment boundary", func() {
		t := ginkgo.GinkgoTB()
		bucket := tgt.uniqueBucket(t, "append-reglock-range")
		client := tgt.pickNode(0)

		// segment 0: bytes 0..4   = "AAAAA"
		// segment 1: bytes 5..9   = "BBBBB"
		// segment 2: bytes 10..14 = "CCCCC"
		// segment 3: bytes 15..19 = "DDDDD"
		segs := [][]byte{
			[]byte("AAAAA"),
			[]byte("BBBBB"),
			[]byte("CCCCC"),
			[]byte("DDDDD"),
		}
		key := "obj-range"
		var off int64
		for _, seg := range segs {
			gomega.Expect(putAppend(client, bucket, key, off, seg)).To(gomega.Succeed())
			off += int64(len(seg))
		}
		fullBody := []byte("AAAAABBBBBCCCCCDDDDDD"[:20])

		// Range [3, 12] spans segments 0–1 (bytes 3,4) and segments 1–2 (bytes 5–9)
		// and the start of segment 2 (bytes 10–12). Expected slice: fullBody[3:13].
		rangeStart := int64(3)
		rangeEnd := int64(12) // inclusive
		rangeBody, err := getObjectRange(client, bucket, key, rangeStart, rangeEnd)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(rangeBody).To(gomega.Equal(fullBody[rangeStart:rangeEnd+1]),
			"range read [%d,%d] must return correct bytes across segment boundaries", rangeStart, rangeEnd)
	})

	// ── Scenario 3 ──────────────────────────────────────────────────────────
	// ListObjectsV2 reports the appendable key with correct total size.
	//
	// BUG (pre-existing, not introduced by this PR): appendable objects are
	// stored in BadgerDB via the raft FSM (CmdAppendObject) and are NOT
	// written to the quorum-meta blob store. scatterGatherList exclusively
	// reads from the quorum-meta store (ScanQuorumMetaBucket), so
	// ListObjectsV2 returns 0 results for appendable-only buckets on the
	// current master. This is exactly the gap Slice 1 (append off-raft) must
	// close: once metadata moves to the quorum-meta store, ListObjects will
	// see appendable objects here.
	//
	// Slice 1 Task 3 (append write path → quorum-meta blob) closed this gap:
	// appendable metadata now lives in the quorum-meta blob, so scatterGatherList
	// (ScanQuorumMetaBucket) sees appendable objects and ListObjectsV2 reports
	// them. Flipped PIt → It; it must stay green (the regression gate). It must
	// never be re-silenced without a fix.
	ginkgo.It("list reports appendable size; delete then 404", func() {
		t := ginkgo.GinkgoTB()
		bucket := tgt.uniqueBucket(t, "append-reglock-list")
		client := tgt.pickNode(0)
		ctx := context.Background()

		chunks := [][]byte{
			[]byte("hello"),
			[]byte(" "),
			[]byte("world"),
		}
		key := "obj-list-del"
		var off int64
		for _, chunk := range chunks {
			gomega.Expect(putAppend(client, bucket, key, off, chunk)).To(gomega.Succeed())
			off += int64(len(chunk))
		}
		expectedSize := off // 11 bytes

		// ListObjectsV2 must include the key with the correct accumulated size.
		listOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(listOut.Contents).To(gomega.HaveLen(1), "bucket should contain exactly one object")
		listedObj := listOut.Contents[0]
		gomega.Expect(aws.ToString(listedObj.Key)).To(gomega.Equal(key))
		gomega.Expect(aws.ToInt64(listedObj.Size)).To(gomega.Equal(expectedSize),
			"ListObjectsV2 size must equal total bytes appended")

		// DeleteObject — must succeed.
		_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "DeleteObject must succeed")

		// Subsequent GetObject must return NoSuchKey (404).
		_, err = client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).To(gomega.HaveOccurred(), "GetObject after delete must fail")
		var apiErr smithy.APIError
		gomega.Expect(errors.As(err, &apiErr)).To(gomega.BeTrue(), "error must be an S3 API error")
		gomega.Expect(apiErr.ErrorCode()).To(gomega.Equal("NoSuchKey"),
			"GetObject after delete must return NoSuchKey")
	})

	// ── Scenario 4 ──────────────────────────────────────────────────────────
	// Coalesce preserves the full body.
	//
	// NOTE: append_coalesce_e2e_test.go / runCoalesceCase already exercises
	// this path for the Cluster4Node fixture with 20 segments and a range read.
	// This explicit It() serves as a named anchor in the regression suite so the
	// description is visible in test output; it delegates to runCoalesceCase to
	// avoid duplication.
	ginkgo.It("coalesce preserves full body", func() {
		runCoalesceCase(ginkgo.GinkgoTB(), tgt)
	})
})

// stripETagQuotes removes surrounding double-quotes from an ETag value.
// The AWS SDK may return the header value with quotes intact (e.g. `"abc-3"`).
func stripETagQuotes(etag string) string {
	if len(etag) >= 2 && etag[0] == '"' && etag[len(etag)-1] == '"' {
		return etag[1 : len(etag)-1]
	}
	return etag
}
