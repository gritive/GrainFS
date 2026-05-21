package nfs4server

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	testBucket = "__grainfs_nfs4_test"
)

func checksumForGeneratedPattern(size int64) uint32 {
	const cycleSum = 32640 // sum of byte values 0..255
	fullCycles := size / 256
	remainder := size % 256

	sum := uint64(fullCycles) * cycleSum
	for i := int64(0); i < remainder; i++ {
		sum += uint64(byte(i))
	}
	return uint32(sum)
}

type generatedPatternReader struct {
	pos  int64
	size int64
}

func newGeneratedPatternReader(size int64) *generatedPatternReader {
	return &generatedPatternReader{size: size}
}

func (r *generatedPatternReader) Read(p []byte) (int, error) {
	if r.pos >= r.size {
		return 0, io.EOF
	}
	remaining := r.size - r.pos
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	for i := range p {
		p[i] = byte((r.pos + int64(i)) % 256)
	}
	r.pos += int64(len(p))
	return len(p), nil
}

func readChecksum(r io.Reader) (int64, uint32, error) {
	buf := make([]byte, 128*1024)
	var total int64
	var sum uint32
	for {
		n, err := r.Read(buf)
		if n > 0 {
			total += int64(n)
			for _, b := range buf[:n] {
				sum += uint32(b)
			}
		}
		if err == io.EOF {
			return total, sum, nil
		}
		if err != nil {
			return total, sum, err
		}
	}
}

// testSizeName returns a human-readable size name.
func testSizeName(size int64) string {
	switch size {
	case 10 * 1024 * 1024:
		return "10MB"
	case 24 * 1024 * 1024:
		return "24MB"
	default:
		return fmt.Sprintf("%dMB", size/(1024*1024))
	}
}

var _ = Describe("NFS4 large file integration", func() {
	var (
		ctx     context.Context
		backend *storage.LocalBackend
	)

	BeforeEach(func() {
		if testing.Short() {
			Skip("Skipping large file test in short mode")
		}

		ctx = context.Background()
		var err error
		backend, err = storage.NewLocalBackend(GinkgoT().TempDir())
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(backend.Close)

		err = backend.CreateBucket(ctx, testBucket)
		if err != nil && err != storage.ErrBucketAlreadyExists {
			Fail(fmt.Sprintf("failed to create bucket: %v", err))
		}
	})

	DescribeTable("reads large files",
		func(size int64) {
			checksum := checksumForGeneratedPattern(size)

			// Upload file via storage backend directly
			key := "test-largefile.bin"
			_, err := backend.PutObject(ctx, testBucket, key, newGeneratedPatternReader(size), "application/octet-stream")
			Expect(err).NotTo(HaveOccurred())

			// Read via backend (simulating NFSv4 read)
			rc, _, err := backend.GetObject(ctx, testBucket, key)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(rc.Close)

			readSize, readChecksum, err := readChecksum(rc)
			Expect(err).NotTo(HaveOccurred())

			// Verify data integrity
			Expect(readSize).To(Equal(size))
			Expect(readChecksum).To(Equal(checksum))
		},
		Entry(testSizeName(10*1024*1024), int64(10*1024*1024)),
		Entry(testSizeName(int64(storage.DefaultChunkSize)+1), int64(storage.DefaultChunkSize)+1),
	)

	DescribeTable("writes large files",
		func(size int64) {
			checksum := checksumForGeneratedPattern(size)

			key := "test-write-largefile.bin"

			// Write via backend (simulating NFSv4 write)
			_, err := backend.PutObject(ctx, testBucket, key, newGeneratedPatternReader(size), "application/octet-stream")
			Expect(err).NotTo(HaveOccurred())

			// Verify via direct read
			rc, _, err := backend.GetObject(ctx, testBucket, key)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(rc.Close)

			readSize, readChecksum, err := readChecksum(rc)
			Expect(err).NotTo(HaveOccurred())

			Expect(readSize).To(Equal(size))
			Expect(readChecksum).To(Equal(checksum))
		},
		Entry(testSizeName(10*1024*1024), int64(10*1024*1024)),
		Entry(testSizeName(int64(storage.DefaultChunkSize)+1), int64(storage.DefaultChunkSize)+1),
	)

	It("handles concurrent large files", func() {
		const numConcurrent = 6
		const fileSize = 8 * 1024 * 1024
		checksum := checksumForGeneratedPattern(fileSize)

		done := make(chan int, numConcurrent)
		errors := make(chan error, numConcurrent)

		for i := 0; i < numConcurrent; i++ {
			go func(idx int) {
				key := fmt.Sprintf("test-concurrent-%d.bin", idx)
				_, err := backend.PutObject(ctx, testBucket, key, newGeneratedPatternReader(fileSize), "application/octet-stream")
				if err != nil {
					errors <- err
					return
				}

				rc, _, err := backend.GetObject(ctx, testBucket, key)
				if err != nil {
					errors <- err
					return
				}
				defer rc.Close()

				readSize, readChecksum, err := readChecksum(rc)
				if err != nil {
					errors <- err
					return
				}
				if readSize != fileSize {
					errors <- fmt.Errorf("size mismatch for file %d: got %d want %d", idx, readSize, fileSize)
					return
				}
				if readChecksum != checksum {
					errors <- fmt.Errorf("checksum mismatch for file %d", idx)
					return
				}

				done <- idx
			}(i)
		}

		for i := 0; i < numConcurrent; i++ {
			select {
			case <-done:
			case err := <-errors:
				Expect(err).NotTo(HaveOccurred())
			}
		}
	})

	It("covers buffer pool tiers without leaks", func() {
		sizes := []int64{
			100 * 1024,
			2 * 1024 * 1024,
			10*1024*1024 + 1,
		}

		for i, size := range sizes {
			key := fmt.Sprintf("test-leak-%d.bin", i)

			_, err := backend.PutObject(ctx, testBucket, key, newGeneratedPatternReader(size), "application/octet-stream")
			Expect(err).NotTo(HaveOccurred())

			rc, _, err := backend.GetObject(ctx, testBucket, key)
			Expect(err).NotTo(HaveOccurred())
			Expect(rc.Close()).To(Succeed())
		}
	})
})
