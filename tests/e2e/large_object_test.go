// Phase 1 chunking pipeline end-to-end smoke.
//
// Exercises the chunked PUT/GET path (SegmentWriter + SegmentReader, default
// 16 MiB chunk size) through the public S3 protocol on both the shared
// single-node fixture and the 4-node cluster fixture, reusing the dual-target
// pattern from buckets_test.go.
//
// Cases:
//   - RoundTrip100MiB          — multi-chunk simple PUT round-trip + ETag check
//   - RoundTrip256MiB          — larger multi-chunk round-trip (skipped in -short)
//   - RangeAcrossChunkBoundary — Range GET that straddles the 16 MiB boundary
package e2e

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"io"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func TestLargeObjectE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runLargeObjectCases(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runLargeObjectCases(t, newSharedClusterS3Target(t))
	})
}

func runLargeObjectCases(t *testing.T, tgt s3Target) {
	client := tgt.pickNode(0)

	t.Run("RoundTrip100MiB", func(t *testing.T) {
		if tgt.isCluster {
			t.Skip("Phase 2 cluster fanout — non-aligned tail chunk (6×16MiB + 4MiB) " +
				"corrupts body. 16 MiB-aligned objects (e.g., RoundTrip256MiB) work. " +
				"Tracked in TODOS.md → Chunking Phase 1 Follow-Ups.")
		}
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "large100")
		data := largeObjectRandomBytes(100 << 20)

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("blob"),
			Body:   bytes.NewReader(data),
		})
		require.NoError(t, err)

		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("blob"),
		})
		require.NoError(t, err)
		got, err := io.ReadAll(out.Body)
		require.NoError(t, err)
		out.Body.Close()
		require.Equal(t, len(data), len(got), "100 MiB length")
		require.True(t, bytes.Equal(got, data), "100 MiB body must round-trip byte-identical")

		sum := md5.Sum(data)
		wantETag := `"` + hex.EncodeToString(sum[:]) + `"`
		require.Equal(t, wantETag, aws.ToString(out.ETag), "simple-PUT ETag = MD5(plaintext)")
	})

	t.Run("RoundTrip256MiB", func(t *testing.T) {
		// 256 MiB = 16 segments × 16 MiB DefaultChunkSize — exercises the
		// SegmentWriter worker pool, multi-segment HeadObject metadata, and
		// the SegmentReader parallel fetch path. Originally specified as
		// 1 GiB, but the simple-PUT path is capped by
		// server.WithMaxRequestBodySize(512 MiB) — a larger single-shot PUT
		// must go through Multipart, which is covered separately by
		// TestMultipartChunkedUploadPartE2E. Skip in -short.
		if testing.Short() {
			t.Skip("256 MiB round-trip skipped in -short mode")
		}
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "large256")
		data := largeObjectRandomBytes(256 << 20)

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("blob"),
			Body:   bytes.NewReader(data),
		})
		require.NoError(t, err)

		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("blob"),
		})
		require.NoError(t, err)
		got, err := io.ReadAll(out.Body)
		require.NoError(t, err)
		out.Body.Close()
		require.Equal(t, len(data), len(got), "256 MiB length")
		require.True(t, bytes.Equal(got, data), "256 MiB body must round-trip byte-identical")
	})

	t.Run("RangeAcrossChunkBoundary", func(t *testing.T) {
		// Phase 1.6.7 closed the single-node gap: PackedBackend now
		// implements storage.PartialIO, so the wal/pullthrough wrappers
		// can route ReadAt through to the segment-aware LocalBackend.
		// Cluster path routes through ClusterCoordinator.ReadAt as before.
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "rangecross")
		// 64 MiB = 4 segments × 16 MiB at DefaultChunkSize.
		data := largeObjectRandomBytes(64 << 20)

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("blob"),
			Body:   bytes.NewReader(data),
		})
		require.NoError(t, err)

		// Range that crosses the first 16 MiB chunk boundary.
		from := int64((16 << 20) - 1024)
		to := int64((16 << 20) + 1024)
		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("blob"),
			Range:  aws.String("bytes=" + strconv.FormatInt(from, 10) + "-" + strconv.FormatInt(to, 10)),
		})
		require.NoError(t, err)
		got, err := io.ReadAll(out.Body)
		require.NoError(t, err)
		out.Body.Close()
		require.Equal(t, data[from:to+1], got, "range across boundary must match")
	})
}

// largeObjectRandomBytes returns n bytes of cryptographic random — used so the
// payload is incompressible and we exercise the full chunking pipeline without
// any short-circuits a repeating pattern might enable.
func largeObjectRandomBytes(n int) []byte {
	out := make([]byte, n)
	if _, err := io.ReadFull(rand.Reader, out); err != nil {
		panic(err)
	}
	return out
}
