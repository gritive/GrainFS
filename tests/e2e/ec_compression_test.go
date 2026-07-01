// EC zstd compression e2e — PUT/GET parity, range GET, ETag stability.
//
// Exercises the zstd segment compression path (store-compressed, decompress on
// read) through the public S3 API on both a single-node fixture and a 4-node
// cluster fixture. Three cases per fixture:
//
//  1. Compressible (~54 MiB) PUT→GET byte-for-byte parity + plaintext ETag
//     unchanged by compression.
//  2. Incompressible (random 40 MiB) PUT→GET parity — exercises the
//     StoredSize==0 raw-stored path where zstd grows the frame and the
//     compressor falls back to storing plaintext.
//  3. Range GET across EC segment boundaries on compressible data — verifies
//     that the decompressor correctly reconstructs the requested slice.
package e2e

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("EC zstd compression", func() {
	describeECCompressionTarget("SingleNode", func(testing.TB) s3Target {
		return newSingleNodeS3Target()
	})
	describeECCompressionTarget("Cluster4Node", func(t testing.TB) s3Target {
		return newSharedClusterS3Target(t)
	})
})

func describeECCompressionTarget(name string, factory func(testing.TB) s3Target) {
	ginkgo.Context(name, func() {
		var tgt s3Target

		ginkgo.BeforeEach(func() {
			tgt = factory(ginkgo.GinkgoTB())
		})

		runECCompressionCases(func() s3Target { return tgt })
	})
}

func runECCompressionCases(getTgt func() s3Target) {
	ginkgo.It("round-trips a compressible object byte-for-byte with stable ETag", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		ctx := context.Background()
		cli := tgt.pickNode(0)
		bucket := tgt.uniqueBucket(t, "ec-cmp-compressible")

		// ~54 MiB: highly compressible, spans more than 3 × 16 MiB EC segments.
		body := bytes.Repeat([]byte("grainfs-compressible-block-"), 2_000_000)

		put, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("c.bin"),
			Body:   bytes.NewReader(body),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		get, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("c.bin"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(get.Body.Close)

		got, err := io.ReadAll(get.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(got).To(gomega.HaveLen(len(body)))
		requireByteEqual(t, body, got, "compressible body must round-trip byte-identical")

		// ETag must be derived from the plaintext content, not the compressed
		// bytes — compression must be transparent to the S3 protocol.
		sum := md5.Sum(body)
		wantETag := `"` + hex.EncodeToString(sum[:]) + `"`
		gomega.Expect(aws.ToString(get.ETag)).To(gomega.Equal(wantETag), "GET ETag = MD5(plaintext)")
		gomega.Expect(aws.ToString(get.ETag)).To(gomega.Equal(aws.ToString(put.ETag)), "PUT ETag == GET ETag: compression must not change ETag")
	})

	ginkgo.It("round-trips an incompressible object (raw-stored path)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		ctx := context.Background()
		cli := tgt.pickNode(0)
		bucket := tgt.uniqueBucket(t, "ec-cmp-random")

		// 40 MiB random data — the zstd frame would be larger than plaintext,
		// so the compressor stores the segment uncompressed (StoredSize == 0).
		body := largeObjectRandomBytes(40 << 20)

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("r.bin"),
			Body:   bytes.NewReader(body),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		get, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("r.bin"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(get.Body.Close)

		got, err := io.ReadAll(get.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(got).To(gomega.HaveLen(len(body)))
		requireByteEqual(t, body, got, "incompressible body must round-trip byte-identical")
	})

	ginkgo.It("serves a correct byte range from a compressible object", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		ctx := context.Background()
		cli := tgt.pickNode(0)
		bucket := tgt.uniqueBucket(t, "ec-cmp-range")

		// ~48 MiB: 3 × 16 MiB segments of repeating pattern so bytes are
		// deterministic at every position.
		body := bytes.Repeat([]byte("0123456789abcdef"), 3_000_000)

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("rng.bin"),
			Body:   bytes.NewReader(body),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Range straddles the second segment boundary (~32 MiB) to exercise
		// decompress + offset slice within a segment.
		const start, end = 20_000_123, 20_001_123
		get, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("rng.bin"),
			Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(get.Body.Close)

		got, err := io.ReadAll(get.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(got).To(gomega.Equal(body[start:end+1]),
			"range [%d-%d] must match plaintext slice", start, end)
	})
}
