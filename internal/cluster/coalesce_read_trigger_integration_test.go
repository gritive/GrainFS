package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Coalesce read and trigger integration", func() {
	var (
		b           *DistributedBackend
		ctx         context.Context
		bucket, key string
	)

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
		ctx = context.Background()
		bucket, key = "b", "k"
		Expect(b.CreateBucket(ctx, bucket)).To(Succeed())
	})

	configureDistributedEC := func() {
		GinkgoHelper()
		b.SetECConfig(ECConfig{DataShards: 4, ParityShards: 2})
		svc := NewShardService(b.root, nil)
		b.SetShardService(svc, []string{b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr})
	}

	appendFormattedChunks := func(prefix string, start, end int, off *int64, expected *[]byte) {
		GinkgoHelper()
		for i := start; i < end; i++ {
			chunk := []byte(fmt.Sprintf("%s%02d-", prefix, i))
			_, err := b.AppendObject(ctx, bucket, key, *off, bytes.NewReader(chunk))
			Expect(err).NotTo(HaveOccurred())
			*off += int64(len(chunk))
			*expected = append(*expected, chunk...)
		}
	}

	expectCoalesceComplete := func(timeout time.Duration) {
		GinkgoHelper()
		Eventually(func() bool {
			obj, _ := b.HeadObject(ctx, bucket, key)
			return obj != nil && len(obj.Coalesced) == 1 && len(obj.Segments) == 0
		}, timeout, 20*time.Millisecond).Should(BeTrue())
	}

	It("triggers coalesce on segment count", func() {
		cfg := *b.coalesceCfg.Load()
		cfg.SegmentCount = 3
		b.SetCoalesceConfig(cfg)

		for i := 0; i < 3; i++ {
			_, err := b.AppendObject(ctx, bucket, key, currentSize(GinkgoT(), b, bucket, key), bytes.NewReader([]byte("a")))
			Expect(err).NotTo(HaveOccurred())
		}

		Eventually(func() bool {
			obj, _ := b.HeadObject(ctx, bucket, key)
			return obj != nil && len(obj.Segments) == 0 && len(obj.Coalesced) == 1
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())
	})

	It("reads a B3 coalesced object after EC distribution", func() {
		configureDistributedEC()

		var off int64
		var expected []byte
		appendFormattedChunks("c", 0, 16, &off, &expected)
		expectCoalesceComplete(5 * time.Second)

		obj, err := b.HeadObject(ctx, bucket, key)
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Coalesced).To(HaveLen(1))

		localPath := b.coalescedBlobPath(bucket, key, obj.Coalesced[0].CoalescedID)
		_, err = os.Stat(localPath)
		Expect(os.IsNotExist(err)).To(BeTrue(), "owner-local coalesced blob should be removed after EC distribution")

		rc, _, err := b.GetObject(ctx, bucket, key)
		Expect(err).NotTo(HaveOccurred())
		body, err := io.ReadAll(rc)
		Expect(err).NotTo(HaveOccurred())
		Expect(rc.Close()).To(Succeed())
		Expect(body).To(Equal(expected))
	})

	It("reads ranges across coalesced and raw append segments", func() {
		configureDistributedEC()

		var off int64
		var expected []byte
		appendFormattedChunks("c", 0, 16, &off, &expected)
		expectCoalesceComplete(5 * time.Second)

		cfg := *b.coalesceCfg.Load()
		cfg.SegmentCount = 1 << 30
		cfg.SizeBytes = 1 << 30
		cfg.IdleTimeout = time.Hour
		b.SetCoalesceConfig(cfg)

		appendFormattedChunks("r", 16, 20, &off, &expected)

		obj, err := b.HeadObject(ctx, bucket, key)
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Coalesced).To(HaveLen(1))
		Expect(obj.Segments).To(HaveLen(4))
		Expect(obj.Size).To(Equal(int64(len(expected))))

		coalescedSize := obj.Coalesced[0].Size
		cases := []struct {
			name        string
			offset, end int64
		}{
			{"within_coalesced", 5, 50},
			{"coalesced_to_raw_boundary", coalescedSize - 10, coalescedSize + 10},
			{"within_raw_tail", coalescedSize + 4, int64(len(expected))},
			{"full", 0, int64(len(expected))},
		}
		for _, tc := range cases {
			tc := tc
			By("reading range " + tc.name)
			buf := make([]byte, tc.end-tc.offset)
			n, err := b.ReadAt(ctx, bucket, key, tc.offset, buf)
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(len(buf)))
			Expect(buf).To(Equal(expected[tc.offset:tc.end]))
		}
	})
})
