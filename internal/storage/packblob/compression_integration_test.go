package packblob

import (
	"bytes"
	"context"
	"io"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/storage"
)

var _ = Describe("Packblob compression integration", func() {
	var (
		ctx context.Context
		pb  *PackedBackend
	)

	BeforeEach(func() {
		ctx = context.Background()
		tmpDir := GinkgoT().TempDir()

		inner, err := storage.NewLocalBackend(tmpDir + "/inner")
		Expect(err).NotTo(HaveOccurred())

		packed, err := NewPackedBackendWithOptions(inner, tmpDir+"/blobs", 64*1024, PackedBackendOptions{Compress: true})
		Expect(err).NotTo(HaveOccurred())
		pb = packed
		DeferCleanup(pb.Close)

		Expect(pb.CreateBucket(ctx, "test")).To(Succeed())
	})

	It("round-trips compressible objects", func() {
		data := bytes.Repeat([]byte("hello world "), 500)
		_, err := pb.PutObject(ctx, "test", "key1", bytes.NewReader(data), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		rc, obj, err := pb.GetObject(ctx, "test", "key1")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(rc.Close)

		Expect(obj.Size).To(Equal(int64(len(data))))

		got, err := io.ReadAll(rc)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(data))
	})

	It("passes large objects through", func() {
		data := bytes.Repeat([]byte("X"), 128*1024)
		_, err := pb.PutObject(ctx, "test", "large", bytes.NewReader(data), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())

		rc, obj, err := pb.GetObject(ctx, "test", "large")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(rc.Close)

		Expect(obj.Size).To(Equal(int64(len(data))))

		got, err := io.ReadAll(rc)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(data))
	})
})
