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
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/require"
)

var _ = ginkgo.Describe("Large objects", func() {
	describeLargeObjectTarget("SingleNode", func(testing.TB) s3Target {
		return newSingleNodeS3Target()
	})
	describeLargeObjectTarget("Cluster4Node", func(t testing.TB) s3Target {
		return newSharedClusterS3Target(t)
	})

	ginkgo.Context("Cluster4Node extras", func() {
		var tgt s3Target

		ginkgo.BeforeEach(func() {
			tgt = newSharedClusterS3Target(ginkgo.GinkgoTB())
		})

		ginkgo.It("round-trips a 64MiB object", func() {
			runLargeObjectRoundTrip(ginkgo.GinkgoTB(), tgt, "large64", 64<<20)
		})
		ginkgo.It("records chunk fanout breadth", func() {
			runClusterFanoutBreadth(ginkgo.GinkgoTB(), tgt)
		})
		ginkgo.It("reads a range across a chunk boundary", func() {
			runLargeObjectRangeAcrossChunkBoundary(ginkgo.GinkgoTB(), tgt)
		})
		ginkgo.It("keeps appendable objects working", func() {
			runLargeObjectClusterAppendable(ginkgo.GinkgoTB(), tgt)
		})
	})
})

func describeLargeObjectTarget(name string, factory func(testing.TB) s3Target) {
	ginkgo.Context(name, func() {
		var tgt s3Target

		ginkgo.BeforeEach(func() {
			tgt = factory(ginkgo.GinkgoTB())
		})

		runLargeObjectCases(func() s3Target { return tgt })
	})
}

func runLargeObjectCases(getTgt func() s3Target) {
	ginkgo.It("round-trips a 100MiB object", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := tgt.pickNode(0)
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
		ginkgo.DeferCleanup(out.Body.Close)
		got, err := io.ReadAll(out.Body)
		require.NoError(t, err)
		require.Equal(t, len(data), len(got), "100 MiB length")
		requireByteEqual(t, data, got, "100 MiB body must round-trip byte-identical")

		sum := md5.Sum(data)
		wantETag := `"` + hex.EncodeToString(sum[:]) + `"`
		require.Equal(t, wantETag, aws.ToString(out.ETag), "simple-PUT ETag = MD5(plaintext)")
	})

	ginkgo.It("round-trips a 256MiB object", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := tgt.pickNode(0)
		// 256 MiB = 16 segments × 16 MiB DefaultChunkSize — exercises the
		// SegmentWriter worker pool, multi-segment HeadObject metadata, and
		// the SegmentReader parallel fetch path. Originally specified as
		// 1 GiB, but the simple-PUT path is capped by
		// server.WithMaxRequestBodySize(512 MiB) — a larger single-shot PUT
		// must go through Multipart, which is covered separately by
		// TestMultipartChunkedUploadPartE2E. Skip in -short.
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
		ginkgo.DeferCleanup(out.Body.Close)
		got, err := io.ReadAll(out.Body)
		require.NoError(t, err)
		require.Equal(t, len(data), len(got), "256 MiB length")
		requireByteEqual(t, data, got, "256 MiB body must round-trip byte-identical")
	})

	ginkgo.It("reads a range across a chunk boundary", func() {
		runLargeObjectRangeAcrossChunkBoundary(ginkgo.GinkgoTB(), getTgt())
	})
}

func runLargeObjectRoundTrip(t testing.TB, tgt s3Target, bucketCase string, size int) {
	t.Helper()
	ctx := context.Background()
	client := tgt.pickNode(0)
	bucket := tgt.uniqueBucket(t, bucketCase)
	data := largeObjectRandomBytes(size)

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
	ginkgo.DeferCleanup(out.Body.Close)
	got, err := io.ReadAll(out.Body)
	require.NoError(t, err)
	require.Equal(t, len(data), len(got), "%d-byte length", size)
	requireByteEqual(t, data, got, "%d-byte body must round-trip byte-identical", size)

	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("blob"),
	})
	require.NoError(t, err)
	require.Equal(t, aws.ToString(out.ETag), aws.ToString(head.ETag), "ETag must remain stable across GET and HEAD")

	sum := md5.Sum(data)
	wantETag := `"` + hex.EncodeToString(sum[:]) + `"`
	require.Equal(t, wantETag, aws.ToString(out.ETag), "simple-PUT ETag = MD5(plaintext)")
}

func requireByteEqual(t testing.TB, want, got []byte, msgAndArgs ...any) {
	t.Helper()
	if bytes.Equal(got, want) {
		return
	}
	limit := len(want)
	if len(got) < limit {
		limit = len(got)
	}
	for i := 0; i < limit; i++ {
		if got[i] != want[i] {
			windowEnd := i + 32
			if windowEnd > limit {
				windowEnd = limit
			}
			gotAt := bytes.Index(want, got[i:windowEnd])
			wantAt := bytes.Index(got, want[i:windowEnd])
			t.Fatalf(
				"first mismatch at byte offset %d: got 0x%02x want 0x%02x; got window found in expected at %d; want window found in got at %d; got=%s want=%s",
				i,
				got[i],
				want[i],
				gotAt,
				wantAt,
				fmt.Sprintf("%x", got[i:windowEnd]),
				fmt.Sprintf("%x", want[i:windowEnd]),
			)
			return
		}
	}
	require.Equal(t, want, got, msgAndArgs...)
}

func runClusterFanoutBreadth(t testing.TB, tgt s3Target) {
	t.Helper()
	require.True(t, tgt.isCluster, "fan-out breadth case requires cluster fixture")
	ctx := context.Background()
	client := tgt.pickNode(0)
	bucket := tgt.uniqueBucket(t, "fanoutbreadth")
	data := largeObjectRandomBytes(128 << 20)

	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("blob"),
		Body:   bytes.NewReader(data),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		var totalCount, totalSum float64
		for i := 0; i < tgt.nodes; i++ {
			count, sum, ok := chunkFanoutHistogram(t, tgt, i)
			if ok {
				totalCount += count
				totalSum += sum
			}
		}
		return totalCount >= 1 && totalSum >= 2
	}, 10*time.Second, 200*time.Millisecond, "chunk fan-out breadth histogram was not observed")
}

func runLargeObjectRangeAcrossChunkBoundary(t testing.TB, tgt s3Target) {
	t.Helper()
	// Phase 1.6.7 closed the single-node gap: PackedBackend now
	// implements storage.PartialIO, so the wal/pullthrough wrappers
	// can route ReadAt through to the segment-aware LocalBackend.
	// Cluster path routes through ClusterCoordinator.ReadAt as before.
	ctx := context.Background()
	client := tgt.pickNode(0)
	bucket := tgt.uniqueBucket(t, "rangecross")
	// 64 MiB = 4 segments x 16 MiB at DefaultChunkSize.
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
	ginkgo.DeferCleanup(out.Body.Close)
	got, err := io.ReadAll(out.Body)
	require.NoError(t, err)
	require.Equal(t, data[from:to+1], got, "range across boundary must match")
}

func runLargeObjectClusterAppendable(t testing.TB, tgt s3Target) {
	t.Helper()
	require.True(t, tgt.isCluster, "appendable case requires cluster fixture")
	bucket := tgt.uniqueBucket(t, "appendable")
	client := tgt.pickNode(0)
	key := "appendable"
	require.NoError(t, putAppend(client, bucket, key, 0, []byte("large-object-")))
	require.NoError(t, putAppend(client, bucket, key, int64(len("large-object-")), []byte("append")))
	require.Equal(t, []byte("large-object-append"), getObject(t, client, bucket, key))
}

func chunkFanoutHistogram(t testing.TB, tgt s3Target, nodeIdx int) (count, sum float64, ok bool) {
	t.Helper()
	resp, err := http.Get(tgt.endpoint(nodeIdx) + "/metrics")
	if err != nil {
		return 0, 0, false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, false
	}

	countLine := "grainfs_chunk_fanout_breadth_count"
	sumLine := "grainfs_chunk_fanout_breadth_sum"
	for _, line := range strings.Split(string(body), "\n") {
		switch {
		case strings.HasPrefix(line, countLine+" "):
			if v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, countLine)), 64); err == nil {
				count = v
			}
		case strings.HasPrefix(line, sumLine+" "):
			if v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, sumLine)), 64); err == nil {
				sum = v
			}
		}
	}
	return count, sum, count > 0
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
