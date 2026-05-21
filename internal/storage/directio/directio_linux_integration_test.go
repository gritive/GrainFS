//go:build linux && directio_integration

package directio

import (
	"bytes"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Linux DirectIO integration", func() {
	It("round-trips an aligned write", func() {
		dir := os.Getenv("GRAINFS_DIRECTIO_TEST_DIR")
		if dir == "" {
			dir = GinkgoT().TempDir()
		} else {
			Expect(os.MkdirAll(dir, 0o755)).To(Succeed())
		}

		path := filepath.Join(dir, "directio-round-trip")
		payload := bytes.Repeat([]byte("grainfs-directio-"), 513)

		f, err := OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		Expect(err).NotTo(HaveOccurred(), "directio integration target must support Linux O_DIRECT")
		DeferCleanup(f.Close)

		buf, alignedLen := AlignedCopy(payload)
		Expect(alignedLen % PageSize()).To(Equal(0))

		n, err := f.Write(buf)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(alignedLen))
		Expect(f.Sync()).To(Succeed())
		Expect(f.Truncate(int64(len(payload)))).To(Succeed())
		Expect(f.Close()).To(Succeed())

		got, err := os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(payload))
	})
})
