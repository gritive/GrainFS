package cluster

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"os"

	"github.com/gritive/GrainFS/internal/storage"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Coalesce merge integration", func() {
	var b *DistributedBackend

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
	})

	It("merges owner-local append segments into a coalesced blob", func() {
		const bucket = "b"
		const key = "k"

		s1, err := b.writeSegmentBlobForAppend(bucket, key, bytes.NewReader([]byte("aaaa")))
		Expect(err).NotTo(HaveOccurred())
		s2, err := b.writeSegmentBlobForAppend(bucket, key, bytes.NewReader([]byte("bbbb")))
		Expect(err).NotTo(HaveOccurred())
		s3, err := b.writeSegmentBlobForAppend(bucket, key, bytes.NewReader([]byte("cc")))
		Expect(err).NotTo(HaveOccurred())

		coalescedID := "c1"
		out, err := b.mergeSegmentsOwnerLocal(bucket, key, coalescedID, []storage.SegmentRef{s1, s2, s3})
		Expect(err).NotTo(HaveOccurred())
		Expect(out.Size).To(Equal(int64(10)))

		sum := md5.Sum([]byte("aaaabbbbcc"))
		Expect(out.ETag).To(Equal(hex.EncodeToString(sum[:])))

		info, err := os.Stat(b.coalescedBlobPath(bucket, key, coalescedID))
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Size()).To(Equal(int64(10)))
	})
})
