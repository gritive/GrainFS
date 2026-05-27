package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Coalesce recovery integration", func() {
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
		enc := testEncryptor(GinkgoT())
		svc := NewShardService(b.root, nil, WithEncryptor(enc), withTestWALEnc(GinkgoT(), enc))
		b.SetShardService(svc, []string{b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr})
	}

	appendECTriggerChunks := func() []byte {
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
		return expected
	}

	putAppendAt := func(off int64, data []byte) {
		GinkgoHelper()
		_, err := b.AppendObject(context.Background(), bucket, key, off, bytes.NewReader(data))
		Expect(err).NotTo(HaveOccurred())
	}

	expectBodySizeMatchesObject := func() {
		GinkgoHelper()
		obj, err := b.HeadObject(ctx, bucket, key)
		Expect(err).NotTo(HaveOccurred())
		rc, _, err := b.GetObject(ctx, bucket, key)
		Expect(err).NotTo(HaveOccurred())
		body, err := io.ReadAll(rc)
		Expect(err).NotTo(HaveOccurred())
		Expect(rc.Close()).To(Succeed())
		Expect(int64(len(body))).To(Equal(obj.Size))
	}

	It("recovers when B3 coalesce crashes after EC write before metadata commit", func() {
		configureDistributedEC()

		cfg := *b.coalesceCfg.Load()
		cfg.SegmentCount = 1 << 30
		cfg.SizeBytes = 1 << 30
		cfg.IdleTimeout = time.Hour
		b.SetCoalesceConfig(cfg)

		expected := appendECTriggerChunks()

		var faulted atomic.Bool
		b.coalesceFaultAfterECWrite = func() error {
			faulted.Store(true)
			return fmt.Errorf("simulated crash")
		}
		err := b.processCoalesceJobB3(ctx, coalesceJob{Bucket: bucket, Key: key})
		Expect(err).To(HaveOccurred())
		Expect(faulted.Load()).To(BeTrue())

		obj, err := b.HeadObject(ctx, bucket, key)
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Coalesced).To(BeEmpty())
		Expect(obj.Segments).To(HaveLen(16))

		b.coalesceFaultAfterECWrite = nil
		Expect(b.processCoalesceJobB3(ctx, coalesceJob{Bucket: bucket, Key: key})).To(Succeed())

		obj, err = b.HeadObject(ctx, bucket, key)
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Coalesced).To(HaveLen(1))
		Expect(obj.Segments).To(BeEmpty())

		rc, _, err := b.GetObject(ctx, bucket, key)
		Expect(err).NotTo(HaveOccurred())
		body, err := io.ReadAll(rc)
		Expect(err).NotTo(HaveOccurred())
		Expect(rc.Close()).To(Succeed())
		Expect(body).To(Equal(expected))
	})

	It("preserves body size while appends and coalesce interleave", func() {
		cfg := *b.coalesceCfg.Load()
		cfg.SegmentCount = 8
		b.SetCoalesceConfig(cfg)

		var totalLen int64
		for i := 0; i < 16; i++ {
			chunk := []byte(fmt.Sprintf("c%02d.", i))
			putAppendAt(totalLen, chunk)
			atomic.AddInt64(&totalLen, int64(len(chunk)))
		}

		Eventually(func() bool {
			obj, _ := b.HeadObject(ctx, bucket, key)
			return obj != nil && len(obj.Coalesced) >= 1
		}, 5*time.Second, 20*time.Millisecond).Should(BeTrue())

		expectBodySizeMatchesObject()
	})

	It("keeps a raced append visible after B2 coalesce", func() {
		cfg := *b.coalesceCfg.Load()
		cfg.SegmentCount = 1 << 30
		b.SetCoalesceConfig(cfg)

		putAppendAt(0, []byte("aa"))
		putAppendAt(2, []byte("bb"))

		pre, err := b.HeadObject(ctx, bucket, key)
		Expect(err).NotTo(HaveOccurred())
		Expect(pre.Segments).To(HaveLen(2))

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			putAppendAt(4, []byte("cc"))
		}()
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			Expect(b.processCoalesceJobB2(ctx, coalesceJob{Bucket: bucket, Key: key})).To(Succeed())
		}()
		wg.Wait()

		expectBodySizeMatchesObject()
		obj, err := b.HeadObject(ctx, bucket, key)
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Size).To(Equal(int64(6)))
	})

	It("reads B3 coalesced data after owner-local files disappear", func() {
		configureDistributedEC()
		expected := appendECTriggerChunks()

		Eventually(func() bool {
			obj, _ := b.HeadObject(ctx, bucket, key)
			return obj != nil && len(obj.Coalesced) == 1 && len(obj.Segments) == 0
		}, 5*time.Second, 20*time.Millisecond).Should(BeTrue())

		segDir := b.objectPath(bucket, key) + "_segments"
		Expect(os.RemoveAll(segDir)).To(Succeed())
		coalescedDir := filepath.Dir(b.coalescedBlobPath(bucket, key, "x"))
		Expect(os.RemoveAll(coalescedDir)).To(Succeed())

		rc, _, err := b.GetObject(ctx, bucket, key)
		Expect(err).NotTo(HaveOccurred())
		body, err := io.ReadAll(rc)
		Expect(err).NotTo(HaveOccurred())
		Expect(rc.Close()).To(Succeed())
		Expect(body).To(Equal(expected))
	})
})
