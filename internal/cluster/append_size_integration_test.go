package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/storage"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Append size integration", func() {
	const bucket = "b"

	var (
		b   *DistributedBackend
		ctx context.Context
	)

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
		ctx = context.Background()
		Expect(b.CreateBucket(ctx, bucket)).To(Succeed())
		b.SetCoalesceConfig(CoalesceConfig{
			SegmentCount: 1 << 30,
			SizeBytes:    1 << 60,
			IdleTimeout:  1 << 60,
			SizeCapBytes: 8,
		})
	})

	It("fast-rejects oversize appends in the coordinator pre-check", func() {
		const key = "k"

		_, err := b.AppendObject(ctx, bucket, key, 0, bytes.NewReader([]byte("abcdefg")))
		Expect(err).NotTo(HaveOccurred())

		_, err = b.AppendObject(ctx, bucket, key, 7, bytes.NewReader([]byte("12345")))
		Expect(errors.Is(err, storage.ErrAppendObjectTooLarge)).To(BeTrue(), "over-cap append must surface ErrAppendObjectTooLarge")
	})

	It("rejects oversize appends at the FSM authoritative cap check", func() {
		const key = "k"

		_, err := b.AppendObject(ctx, bucket, key, 0, bytes.NewReader([]byte("hello")))
		Expect(err).NotTo(HaveOccurred())

		_, err = b.AppendObject(ctx, bucket, key, 5, bytes.NewReader([]byte("more")))
		Expect(errors.Is(err, storage.ErrAppendObjectTooLarge)).To(BeTrue(), "second append over cap must surface ErrAppendObjectTooLarge")
	})

	It("allows an append that fills the cap exactly and rejects the next byte", func() {
		const key = "exactly8"

		_, err := b.AppendObject(ctx, bucket, key, 0, bytes.NewReader([]byte("abcdefgh")))
		Expect(err).NotTo(HaveOccurred())

		_, err = b.AppendObject(ctx, bucket, key, 8, bytes.NewReader([]byte("x")))
		Expect(errors.Is(err, storage.ErrAppendObjectTooLarge)).To(BeTrue())
	})

	It("observes append size cap and coalesced size metrics", func() {
		_, err := b.AppendObject(ctx, bucket, "k-cap", 0, &noSeekReader{bytes.NewReader([]byte("hello"))})
		Expect(err).NotTo(HaveOccurred())

		rejBefore := testutil.ToFloat64(metrics.AppendSizeCapRejectedTotal)
		_, err = b.AppendObject(ctx, bucket, "k-cap", 5, &noSeekReader{bytes.NewReader([]byte("more"))})
		Expect(err).To(HaveOccurred())
		rejAfter := testutil.ToFloat64(metrics.AppendSizeCapRejectedTotal)
		Expect(rejAfter-rejBefore).To(BeNumerically(">=", 1.0), "AppendSizeCapRejectedTotal must increment")

		b.SetCoalesceConfig(CoalesceConfig{
			SegmentCount: 1 << 30,
			SizeBytes:    1 << 60,
			IdleTimeout:  1 << 60,
			SizeCapBytes: 0,
		})
		depthCountBefore := appendHistogramSampleCount("grainfs_append_coalesced_depth")
		bytesCountBefore := appendHistogramSampleCount("grainfs_append_coalesced_total_bytes")

		_, err = b.AppendObject(ctx, bucket, "k-ok", 0, bytes.NewReader([]byte("hi")))
		Expect(err).NotTo(HaveOccurred())

		depthCountAfter := appendHistogramSampleCount("grainfs_append_coalesced_depth")
		bytesCountAfter := appendHistogramSampleCount("grainfs_append_coalesced_total_bytes")
		Expect(depthCountAfter).To(BeNumerically(">", depthCountBefore), "AppendCoalescedDepth must receive a sample")
		Expect(bytesCountAfter).To(BeNumerically(">", bytesCountBefore), "AppendCoalescedTotalBytes must receive a sample")
	})
})

func appendHistogramSampleCount(name string) uint64 {
	mfs, err := prometheus.DefaultGatherer.Gather()
	Expect(err).NotTo(HaveOccurred())
	for _, mf := range mfs {
		if mf.GetName() == name {
			for _, m := range mf.GetMetric() {
				if h := m.GetHistogram(); h != nil {
					return h.GetSampleCount()
				}
			}
		}
	}
	return 0
}

type noSeekReader struct{ r io.Reader }

func (n *noSeekReader) Read(p []byte) (int, error) { return n.r.Read(p) }
