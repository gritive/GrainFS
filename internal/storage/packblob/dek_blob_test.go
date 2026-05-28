package packblob

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/encrypt"
)

var _ = ginkgo.Describe("packblob DEK blob store", func() {
	var keeper *encrypt.DEKKeeper
	ginkgo.BeforeEach(func() {
		kek := make([]byte, encrypt.KEKSize)
		clusterID := make([]byte, 16)
		var err error
		keeper, err = encrypt.NewDEKKeeper(kek, clusterID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("rejects a nil keeper", func() {
		_, err := NewDEKBlobStore(ginkgo.GinkgoT().TempDir(), 256*1024*1024, nil, make([]byte, 16))
		gomega.Expect(err).To(gomega.HaveOccurred())
	})

	ginkgo.It("requires a 16-byte clusterID", func() {
		_, err := NewDEKBlobStore(ginkgo.GinkgoT().TempDir(), 256*1024*1024, keeper, make([]byte, 8))
		gomega.Expect(err).To(gomega.HaveOccurred())
	})

	ginkgo.It("round-trips an entry sealed under the active DEK gen", func() {
		dir := ginkgo.GinkgoT().TempDir()
		bs, err := NewDEKBlobStore(dir, 256*1024*1024, keeper, make([]byte, 16))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		loc, err := bs.Append("k", []byte("payload"))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		got, err := bs.Read(loc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(got).To(gomega.Equal([]byte("payload")))
	})

	ginkgo.It("seals under the active DEK gen 0 (packblob gen-frame invariant)", func() {
		// packblob discards the seal gen on write and always opens at gen 0, so it
		// is correct ONLY while the active gen is 0. R1 gates encryption.rotate-dek
		// to keep this true.
		gomega.Expect(keeper.ActiveDEKGeneration()).To(gomega.Equal(uint32(0)))
	})
})
