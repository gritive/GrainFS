package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/storage"
)

var _ = Describe("Zero-copy sendfile integration", func() {
	var (
		ctx     context.Context
		backend *storage.LocalBackend
	)

	BeforeEach(func() {
		ctx = context.Background()
		tmpDir, err := os.MkdirTemp("", "grainfs-e2e-zerocopy-*")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(os.RemoveAll, tmpDir)

		backend, err = storage.NewLocalBackend(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(backend.Close)
		Expect(backend.CreateBucket(ctx, "test-bucket")).To(Succeed())
	})

	startServer := func() string {
		port := freePort(GinkgoT())
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		s := New(addr, backend)
		DeferCleanup(shutdownTestServer, GinkgoT(), s)
		go func() {
			if err := s.Run(); err != nil && err != http.ErrServerClosed {
				GinkgoWriter.Printf("server error: %v\n", err)
			}
		}()
		waitForTCP(GinkgoT(), addr)
		return "http://" + addr
	}

	putPublicObject := func(key string, data []byte) {
		_, err := backend.PutObject(ctx, "test-bucket", key, bytes.NewReader(data), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
		Expect(backend.SetObjectACL("test-bucket", key, 1)).To(Succeed())
	}

	DescribeTable("preserves object data integrity",
		func(size int) {
			originalData := bytes.Repeat([]byte("X"), size)
			expectedChecksum := fmt.Sprintf("%x", simpleChecksum(originalData))

			key := fmt.Sprintf("test-%d", size)
			_, err := backend.PutObject(ctx, "test-bucket", key, bytes.NewReader(originalData), "application/octet-stream")
			Expect(err).NotTo(HaveOccurred())

			rc, _, err := backend.GetObject(ctx, "test-bucket", key)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(rc.Close)

			downloadedData, err := io.ReadAll(rc)
			Expect(err).NotTo(HaveOccurred())

			Expect(fmt.Sprintf("%x", simpleChecksum(downloadedData))).To(Equal(expectedChecksum))
			Expect(downloadedData).To(Equal(originalData))
		},
		Entry("1KB", 1*1024),
		Entry("16KB", 16*1024),
		Entry("16KB+1", 16*1024+1),
		Entry("32KB", 32*1024),
		Entry("64KB", 64*1024),
		Entry("1MB", 1024*1024),
	)

	It("serves concurrent zero-copy reads without data mismatch", func() {
		testData := bytes.Repeat([]byte("C"), 64*1024)
		_, err := backend.PutObject(ctx, "test-bucket", "concurrent", bytes.NewReader(testData), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())

		const numGoroutines = 100
		errors := make(chan error, numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func() {
				rc, _, err := backend.GetObject(ctx, "test-bucket", "concurrent")
				if err != nil {
					errors <- fmt.Errorf("get object failed: %w", err)
					return
				}
				defer rc.Close()

				data, err := io.ReadAll(rc)
				if err != nil {
					errors <- fmt.Errorf("read failed: %w", err)
					return
				}
				if !bytes.Equal(data, testData) {
					errors <- fmt.Errorf("data mismatch")
					return
				}
				errors <- nil
			}()
		}

		for i := 0; i < numGoroutines; i++ {
			Expect(<-errors).NotTo(HaveOccurred())
		}
	})

	It("round-trips multiple large files in sequence", func() {
		const (
			numFiles = 12
			fileSize = 128 * 1024
		)
		for i := 0; i < numFiles; i++ {
			data := bytes.Repeat([]byte(fmt.Sprintf("%d", i%10)), fileSize)
			key := fmt.Sprintf("file-%04d", i)
			_, err := backend.PutObject(ctx, "test-bucket", key, bytes.NewReader(data), "application/octet-stream")
			Expect(err).NotTo(HaveOccurred())
		}

		for i := 0; i < numFiles; i++ {
			key := fmt.Sprintf("file-%04d", i)
			expectedData := bytes.Repeat([]byte(fmt.Sprintf("%d", i%10)), fileSize)

			rc, _, err := backend.GetObject(ctx, "test-bucket", key)
			Expect(err).NotTo(HaveOccurred())
			data, err := io.ReadAll(rc)
			Expect(rc.Close()).To(Succeed())
			Expect(err).NotTo(HaveOccurred())
			Expect(data).To(Equal(expectedData))
		}
	})

	It("serves small and large public objects through HTTP", func() {
		smallData := bytes.Repeat([]byte("S"), 1*1024)
		largeData := bytes.Repeat([]byte("L"), 64*1024)
		putPublicObject("small", smallData)
		putPublicObject("large", largeData)

		baseURL := startServer()
		client := &http.Client{}

		resp, err := client.Get(baseURL + "/test-bucket/small")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(resp.Body.Close)
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		data, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(Equal(smallData))

		resp, err = client.Get(baseURL + "/test-bucket/large")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(resp.Body.Close)
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		data, err = io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(Equal(largeData))
	})

	It("serves bounded range requests as partial content", func() {
		largeData := bytes.Repeat([]byte("R"), 64*1024)
		putPublicObject("large", largeData)
		baseURL := startServer()

		req, err := http.NewRequest(http.MethodGet, baseURL+"/test-bucket/large", nil)
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("Range", "bytes=0-1023")
		resp, err := http.DefaultClient.Do(req)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(resp.Body.Close)
		Expect(resp.StatusCode).To(Equal(http.StatusPartialContent))
		Expect(resp.Header.Get("Content-Range")).To(Equal("bytes 0-1023/65536"))
		data, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(Equal(largeData[:1024]))

		req, err = http.NewRequest(http.MethodGet, baseURL+"/test-bucket/large", nil)
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("Range", "bytes=1024-2047")
		resp, err = http.DefaultClient.Do(req)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(resp.Body.Close)
		Expect(resp.StatusCode).To(Equal(http.StatusPartialContent))
		data, err = io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(Equal(largeData[1024:2048]))
	})

	It("handles range edge cases through HTTP", func() {
		largeData := bytes.Repeat([]byte("E"), 64*1024)
		putPublicObject("large", largeData)
		baseURL := startServer()

		req, err := http.NewRequest(http.MethodGet, baseURL+"/test-bucket/large", nil)
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("Range", "bytes=99999-100000")
		resp, err := http.DefaultClient.Do(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Body.Close()).To(Succeed())
		Expect(resp.StatusCode).To(Equal(http.StatusRequestedRangeNotSatisfiable))
		Expect(resp.Header.Get("Content-Range")).To(ContainSubstring("bytes */"))

		req, err = http.NewRequest(http.MethodGet, baseURL+"/test-bucket/large", nil)
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("Range", "bytes=1024-")
		resp, err = http.DefaultClient.Do(req)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(resp.Body.Close)
		Expect(resp.StatusCode).To(Equal(http.StatusPartialContent))
		data, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(Equal(largeData[1024:]))

		req, err = http.NewRequest(http.MethodGet, baseURL+"/test-bucket/large", nil)
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("Range", "bytes=-512")
		resp, err = http.DefaultClient.Do(req)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(resp.Body.Close)
		Expect(resp.StatusCode).To(Equal(http.StatusPartialContent))
		data, err = io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(Equal(largeData[len(largeData)-512:]))
	})
})

// simpleChecksum is a simple checksum for testing (not cryptographically secure)
func simpleChecksum(data []byte) byte {
	var checksum byte
	for _, b := range data {
		checksum ^= b
	}
	return checksum
}
