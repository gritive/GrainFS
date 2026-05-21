// UploadPart aws-chunked body decoding regression (multipart partN failure).
//
// warp's multipart workload sends parts with `X-Amz-Content-Sha256:
// STREAMING-AWS4-HMAC-SHA256-PAYLOAD` + aws-chunked body framing. Before the
// fix, `uploadPart` stored the framed bytes (chunk headers + per-chunk
// signatures) as the part payload — `Part.Size` and the cluster object's
// `obj.Size` inflated by the framing overhead and `?partNumber=N` returned
// the framed bytes instead of the caller's plaintext.
//
// aws-sdk-go-v2's `bytes.NewReader` path does NOT trigger the streaming
// transport (the SDK seeks/hashes the in-memory body), so we craft the raw
// HTTP request with SigV4 + aws-chunked framing by hand. Runs against
// SingleNode + Cluster4Node to ensure both paths route through the same
// decoder.
package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const streamingPayloadSentinel = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"

func runMultipartChunkedUploadPartSpecs() {
	ginkgo.Context("ChunkedUploadPart SingleNode", func() {
		var tgt s3Target
		ginkgo.BeforeEach(func() {
			tgt = newSingleNodeS3Target()
		})
		runMultipartChunkedCases(func() s3Target { return tgt })
	})

	ginkgo.Context("ChunkedUploadPart Cluster4Node", func() {
		var tgt s3Target
		ginkgo.BeforeEach(func() {
			tgt = newSharedClusterS3Target(ginkgo.GinkgoTB())
		})
		runMultipartChunkedCases(func() s3Target { return tgt })
	})
}

func runMultipartChunkedCases(getTgt func() s3Target) {
	ginkgo.It("strips aws-chunked framing from uploaded parts", func() {
		t, tgt, client := multipartFixture(getTgt, "mp-chunked-probe")
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "mp-chunked")
		key := "warp-style-multipart.bin"

		// 64 KiB payload — one full warp-shaped chunk + last terminator.
		// Small enough to keep the test fast, big enough that aws-chunked
		// framing adds visible overhead (~90 bytes) we can assert against.
		plaintext := bytes.Repeat([]byte{'A'}, 64*1024)

		initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			ContentType: aws.String("application/octet-stream"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		uploadID := aws.ToString(initOut.UploadId)

		// Upload part 1 via raw chunked HTTP — bypasses aws-sdk-go-v2's
		// hashed in-memory body path so the chunk framing actually hits
		// the server.
		etag := uploadPartChunked(t, ctx, tgt, bucket, key, uploadID, 1, plaintext)

		_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: aws.String(uploadID),
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{
					{PartNumber: aws.Int32(1), ETag: aws.String(etag)},
				},
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Full GET: body must be the plaintext, not the chunked framing.
		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(getOut.Body.Close)
		body, err := io.ReadAll(getOut.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(aws.ToInt64(getOut.ContentLength)).To(gomega.Equal(int64(len(plaintext))),
			"Content-Length must match plaintext (not framed bytes)")
		gomega.Expect(body).To(gomega.HaveLen(len(plaintext)), "body length")
		if len(body) == len(plaintext) {
			gomega.Expect(body).To(gomega.Equal(plaintext), "body bytes")
		}

		// ?partNumber=1 GET: same expectation.
		partOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			PartNumber: aws.Int32(1),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(partOut.Body.Close)
		gomega.Expect(aws.ToInt64(partOut.ContentLength)).To(gomega.Equal(int64(len(plaintext))),
			"?partNumber=1 Content-Length must match plaintext")
		partBody, err := io.ReadAll(partOut.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(partBody).To(gomega.HaveLen(len(plaintext)), "?partNumber=1 body length")
		if len(partBody) == len(plaintext) {
			gomega.Expect(partBody).To(gomega.Equal(plaintext), "?partNumber=1 body bytes")
		}
	})

	ginkgo.It("completes a large upload with a non-aligned final segment", func() {
		t, tgt, client := multipartFixture(getTgt, "mp-chunked-probe")
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "mp-large-complete")
		key := "large-complete.bin"

		part1 := bytes.Repeat([]byte{'A'}, 5<<20)
		part2 := bytes.Repeat([]byte{'B'}, 5<<20)
		part3 := bytes.Repeat([]byte{'C'}, (6<<20)+1)
		want := append(append(append([]byte{}, part1...), part2...), part3...)

		initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			ContentType: aws.String("application/octet-stream"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		uploadID := aws.ToString(initOut.UploadId)

		completed := make([]types.CompletedPart, 0, 3)
		for i, body := range [][]byte{part1, part2, part3} {
			out, err := client.UploadPart(ctx, &s3.UploadPartInput{
				Bucket:     aws.String(bucket),
				Key:        aws.String(key),
				UploadId:   aws.String(uploadID),
				PartNumber: aws.Int32(int32(i + 1)),
				Body:       bytes.NewReader(body),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			completed = append(completed, types.CompletedPart{
				PartNumber: aws.Int32(int32(i + 1)),
				ETag:       out.ETag,
			})
		}

		_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: aws.String(uploadID),
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: completed,
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(getOut.Body.Close)
		got, err := io.ReadAll(getOut.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(aws.ToInt64(getOut.ContentLength)).To(gomega.Equal(int64(len(want))))
		gomega.Expect(got).To(gomega.Equal(want), "body mismatch")
	})

	ginkgo.It("reads correctly when a segment spans a part boundary", func() {
		t, tgt, client := multipartFixture(getTgt, "mp-chunked-probe")
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "mp-segment-boundary")
		key := "segment-spans-part-boundary.bin"

		part1 := bytes.Repeat([]byte{'D'}, 8<<20)
		part2 := bytes.Repeat([]byte{'E'}, 8<<20)
		part3 := []byte("tail")
		want := append(append(append([]byte{}, part1...), part2...), part3...)

		initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		uploadID := aws.ToString(initOut.UploadId)

		completed := make([]types.CompletedPart, 0, 3)
		for i, body := range [][]byte{part1, part2, part3} {
			out, err := client.UploadPart(ctx, &s3.UploadPartInput{
				Bucket:     aws.String(bucket),
				Key:        aws.String(key),
				UploadId:   aws.String(uploadID),
				PartNumber: aws.Int32(int32(i + 1)),
				Body:       bytes.NewReader(body),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			completed = append(completed, types.CompletedPart{
				PartNumber: aws.Int32(int32(i + 1)),
				ETag:       out.ETag,
			})
		}

		_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: aws.String(uploadID),
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: completed,
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(getOut.Body.Close)
		got, err := io.ReadAll(getOut.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(aws.ToInt64(getOut.ContentLength)).To(gomega.Equal(int64(len(want))))
		gomega.Expect(got).To(gomega.Equal(want), "body mismatch")
	})
}

// uploadPartChunked issues an UploadPart request with aws-chunked framing
// and SigV4 header signing (with payload hash = STREAMING-... so the server
// does not require chunk signatures to be valid — only the framing needs to
// parse). Returns the ETag the server reports for the stored part.
func uploadPartChunked(t testing.TB, ctx context.Context, tgt s3Target, bucket, key, uploadID string, partN int, plaintext []byte) string {
	t.Helper()

	// One data chunk + zero-length terminator. Each chunk header carries a
	// dummy 64-hex signature; DecodeAWSChunkedBody only parses the hex size.
	dummySig := "0000000000000000000000000000000000000000000000000000000000000000"
	var body bytes.Buffer
	fmt.Fprintf(&body, "%x;chunk-signature=%s\r\n", len(plaintext), dummySig)
	body.Write(plaintext)
	body.WriteString("\r\n")
	fmt.Fprintf(&body, "0;chunk-signature=%s\r\n\r\n", dummySig)

	url := fmt.Sprintf("%s/%s/%s?partNumber=%d&uploadId=%s",
		tgt.endpoint(0), bucket, key, partN, uploadID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body.Bytes()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.ContentLength = int64(body.Len())
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Decoded-Content-Length", fmt.Sprintf("%d", len(plaintext)))
	req.Header.Set("X-Amz-Content-Sha256", streamingPayloadSentinel)

	signer := v4.NewSigner(func(o *v4.SignerOptions) {
		o.DisableURIPathEscaping = true
	})
	creds := aws.Credentials{AccessKeyID: tgt.accessKey, SecretAccessKey: tgt.secretKey}
	gomega.Expect(signer.SignHTTP(ctx, creds, req, streamingPayloadSentinel, "s3", "us-east-1", time.Now())).To(gomega.Succeed())

	resp, err := http.DefaultClient.Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(resp.Body.Close)
	respBody, _ := io.ReadAll(resp.Body)
	gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK), "UploadPart status (body=%s)", string(respBody))

	etag := resp.Header.Get("ETag")
	gomega.Expect(etag).NotTo(gomega.BeEmpty(), "ETag header on UploadPart response")
	return etag
}
