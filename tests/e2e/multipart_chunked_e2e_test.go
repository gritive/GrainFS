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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const streamingPayloadSentinel = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"

func runMultipartChunkedUploadPart(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runMultipartChunkedCases(t, newSingleNodeS3Target())
	})

	t.Run("Cluster4Node", func(t *testing.T) {
		runMultipartChunkedCases(t, newSharedClusterS3Target(t))
	})
}

func runMultipartChunkedCases(t *testing.T, tgt s3Target) {
	client := tgt.pickNode(0)

	if tgt.isCluster {
		probe := tgt.name + "-mp-chunked-probe"
		tgt.createBkt(t, probe)
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer cancel()
		waitForMultipartListingCreate(t, ctx, client, probe, multipartListingKey, 120*time.Second)
	}

	t.Run("ChunkedFramingStripped", func(t *testing.T) {
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
		require.NoError(t, err)
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
		require.NoError(t, err)

		// Full GET: body must be the plaintext, not the chunked framing.
		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		defer getOut.Body.Close()
		body, err := io.ReadAll(getOut.Body)
		require.NoError(t, err)
		assert.Equal(t, int64(len(plaintext)), aws.ToInt64(getOut.ContentLength),
			"Content-Length must match plaintext (not framed bytes)")
		assert.Equal(t, len(plaintext), len(body), "body length")
		if len(body) == len(plaintext) {
			assert.Equal(t, plaintext, body, "body bytes")
		}

		// ?partNumber=1 GET: same expectation.
		partOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			PartNumber: aws.Int32(1),
		})
		require.NoError(t, err)
		defer partOut.Body.Close()
		assert.Equal(t, int64(len(plaintext)), aws.ToInt64(partOut.ContentLength),
			"?partNumber=1 Content-Length must match plaintext")
		partBody, err := io.ReadAll(partOut.Body)
		require.NoError(t, err)
		assert.Equal(t, len(plaintext), len(partBody), "?partNumber=1 body length")
		if len(partBody) == len(plaintext) {
			assert.Equal(t, plaintext, partBody, "?partNumber=1 body bytes")
		}
	})

	t.Run("LargeCompleteNonAlignedFinalSegment", func(t *testing.T) {
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
		require.NoError(t, err)
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
			require.NoError(t, err)
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
		require.NoError(t, err)

		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		defer getOut.Body.Close()
		got, err := io.ReadAll(getOut.Body)
		require.NoError(t, err)
		require.Equal(t, int64(len(want)), aws.ToInt64(getOut.ContentLength))
		require.True(t, bytes.Equal(want, got), "body mismatch")
	})

	t.Run("SegmentSpansPartBoundary", func(t *testing.T) {
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
		require.NoError(t, err)
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
			require.NoError(t, err)
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
		require.NoError(t, err)

		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		defer getOut.Body.Close()
		got, err := io.ReadAll(getOut.Body)
		require.NoError(t, err)
		require.Equal(t, int64(len(want)), aws.ToInt64(getOut.ContentLength))
		require.True(t, bytes.Equal(want, got), "body mismatch")
	})
}

// uploadPartChunked issues an UploadPart request with aws-chunked framing
// and SigV4 header signing (with payload hash = STREAMING-... so the server
// does not require chunk signatures to be valid — only the framing needs to
// parse). Returns the ETag the server reports for the stored part.
func uploadPartChunked(t *testing.T, ctx context.Context, tgt s3Target, bucket, key, uploadID string, partN int, plaintext []byte) string {
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
	require.NoError(t, err)
	req.ContentLength = int64(body.Len())
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Decoded-Content-Length", fmt.Sprintf("%d", len(plaintext)))
	req.Header.Set("X-Amz-Content-Sha256", streamingPayloadSentinel)

	signer := v4.NewSigner(func(o *v4.SignerOptions) {
		o.DisableURIPathEscaping = true
	})
	creds := aws.Credentials{AccessKeyID: tgt.accessKey, SecretAccessKey: tgt.secretKey}
	require.NoError(t, signer.SignHTTP(ctx, creds, req, streamingPayloadSentinel, "s3", "us-east-1", time.Now()))

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "UploadPart status (body=%s)", string(respBody))

	etag := resp.Header.Get("ETag")
	require.NotEmpty(t, etag, "ETag header on UploadPart response")
	return etag
}
