package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/metrics"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Coalesce EC integration", func() {
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

	configureEC := func(opts ...ShardServiceOption) {
		GinkgoHelper()
		b.SetECConfig(ECConfig{DataShards: 4, ParityShards: 2})
		svc := NewShardService(b.root, nil, opts...)
		b.SetShardService(svc, []string{b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr})
	}

	appendTriggerChunks := func() ([]byte, int64) {
		GinkgoHelper()
		var off int64
		var expected []byte
		for i := 0; i < 16; i++ {
			chunk := []byte(fmt.Sprintf("c%02d-", i))
			_, err := b.AppendObject(ctx, bucket, key, off, bytes.NewReader(chunk))
			Expect(err).NotTo(HaveOccurred())
			off += int64(len(chunk))
			expected = append(expected, chunk...)
		}
		return expected, off
	}

	expectCoalesceComplete := func() {
		GinkgoHelper()
		Eventually(func() bool {
			obj, _ := b.HeadObject(ctx, bucket, key)
			return obj != nil && len(obj.Coalesced) == 1 && len(obj.Segments) == 0
		}, 5*time.Second, 20*time.Millisecond).Should(BeTrue())
	}

	It("distributes B3 coalesced data across the EC placement", func() {
		configureEC()
		appendTriggerChunks()
		expectCoalesceComplete()

		obj, err := b.HeadObject(ctx, bucket, key)
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Coalesced).To(HaveLen(1))
		c := obj.Coalesced[0]

		Expect(c.NodeIDs).To(HaveLen(6))
		Expect(c.ECData).To(Equal(uint8(4)))
		Expect(c.ECParity).To(Equal(uint8(2)))

		localPath := b.coalescedBlobPath(bucket, key, c.CoalescedID)
		_, err = os.Stat(localPath)
		Expect(os.IsNotExist(err)).To(BeTrue(), "owner-local coalesced blob should be removed after EC distribution")
	})

	It("observes successful coalesce metrics", func() {
		configureEC()
		successBefore := testutil.ToFloat64(metrics.AppendCoalesceTotal.WithLabelValues("success"))
		bytesBefore := testutil.ToFloat64(metrics.AppendCoalesceBytes)

		_, totalBytes := appendTriggerChunks()

		Eventually(func() bool {
			obj, _ := b.HeadObject(ctx, bucket, key)
			return obj != nil && len(obj.Coalesced) == 1
		}, 5*time.Second, 20*time.Millisecond).Should(BeTrue())

		Eventually(func() bool {
			successAfter := testutil.ToFloat64(metrics.AppendCoalesceTotal.WithLabelValues("success"))
			return successAfter-successBefore >= 1.0
		}, 5*time.Second, 20*time.Millisecond).Should(BeTrue())

		Eventually(func() bool {
			bytesAfter := testutil.ToFloat64(metrics.AppendCoalesceBytes)
			return bytesAfter-bytesBefore >= float64(totalBytes)
		}, 5*time.Second, 20*time.Millisecond).Should(BeTrue())
	})

	It("round-trips B3 coalesced EC data through encrypted shard storage", func() {
		enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x55}, 32))
		Expect(err).NotTo(HaveOccurred())
		configureEC(WithEncryptor(enc))

		expected, _ := appendTriggerChunks()
		expectCoalesceComplete()

		rc, _, err := b.GetObject(ctx, bucket, key)
		Expect(err).NotTo(HaveOccurred())
		body, err := io.ReadAll(rc)
		Expect(err).NotTo(HaveOccurred())
		Expect(rc.Close()).To(Succeed())
		Expect(body).To(Equal(expected))
	})
})
