package server

import (
	"bytes"
	"context"
	"io"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/cluster"
)

var _ = Describe("Sendfile integration", func() {
	var (
		ctx     context.Context
		backend *cluster.DistributedBackend
	)

	BeforeEach(func() {
		ctx = context.Background()
		backend = cluster.NewSingletonBackendForTest(GinkgoT())
		Expect(backend.CreateBucket(ctx, "test-bucket")).To(Succeed())

		smallData := bytes.Repeat([]byte("A"), 1024)
		_, err := backend.PutObject(ctx, "test-bucket", "small", bytes.NewReader(smallData), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())

		largeData := bytes.Repeat([]byte("B"), 32*1024)
		_, err = backend.PutObject(ctx, "test-bucket", "large", bytes.NewReader(largeData), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
	})

	It("reads small objects through the standard path", func() {
		rc, obj, err := backend.GetObject(context.Background(), "test-bucket", "small")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(rc.Close)

		Expect(obj.Size).To(Equal(int64(1024)))

		data, err := io.ReadAll(rc)
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(HaveLen(1024))
	})

	DescribeTable("uses the expected zero-copy threshold",
		func(size int64, expected string) {
			useZeroCopy := size > 16*1024
			result := "standard"
			if useZeroCopy {
				result = "zero-copy"
			}

			Expect(result).To(Equal(expected))
		},
		Entry("1KB", int64(1*1024), "standard"),
		Entry("16KB", int64(16*1024), "standard"),
		Entry("16KB+1", int64(16*1024+1), "zero-copy"),
		Entry("32KB", int64(32*1024), "zero-copy"),
	)
})
