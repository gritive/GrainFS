package nfs4server

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
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

func TestNFSv4LargeFileRead(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large file test in short mode")
	}

	backend, err := storage.NewLocalBackend(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}
	defer backend.Close()

	sizes := []int64{
		10 * 1024 * 1024,                    // under the 16MiB storage segment size
		int64(storage.DefaultChunkSize) + 1, // crosses the segment boundary
	}

	for _, size := range sizes {
		t.Run(testSizeName(size), func(t *testing.T) {
			checksum := checksumForGeneratedPattern(size)

			// Create bucket first
			err := backend.CreateBucket(context.Background(), testBucket)
			if err != nil && err != storage.ErrBucketAlreadyExists {
				t.Fatalf("failed to create bucket: %v", err)
			}

			// Upload file via storage backend directly
			key := "test-largefile.bin"
			_, err = backend.PutObject(context.Background(), testBucket, key, newGeneratedPatternReader(size), "application/octet-stream")
			if err != nil {
				t.Fatalf("failed to upload test file: %v", err)
			}

			// Read via backend (simulating NFSv4 read)
			start := time.Now()
			rc, _, err := backend.GetObject(context.Background(), testBucket, key)
			if err != nil {
				t.Fatalf("failed to open file: %v", err)
			}
			defer rc.Close()

			readSize, readChecksum, err := readChecksum(rc)
			if err != nil {
				t.Fatalf("failed to read file: %v", err)
			}
			duration := time.Since(start)

			// Verify data integrity
			if readSize != size {
				t.Errorf("size mismatch: got %d, want %d", readSize, size)
			}

			if readChecksum != checksum {
				t.Errorf("checksum mismatch: got %x, want %x", readChecksum, checksum)
			}

			throughput := float64(size) / duration.Seconds()
			t.Logf("Size: %d, Throughput: %.2f MB/s, Duration: %v", size, throughput/(1024*1024), duration)
		})
	}
}

func TestNFSv4LargeFileWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large file test in short mode")
	}

	backend, err := storage.NewLocalBackend(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}
	defer backend.Close()

	sizes := []int64{
		10 * 1024 * 1024,                    // under the 16MiB storage segment size
		int64(storage.DefaultChunkSize) + 1, // crosses the segment boundary
	}

	for _, size := range sizes {
		t.Run(testSizeName(size), func(t *testing.T) {
			checksum := checksumForGeneratedPattern(size)

			// Create bucket first
			err := backend.CreateBucket(context.Background(), testBucket)
			if err != nil && err != storage.ErrBucketAlreadyExists {
				t.Fatalf("failed to create bucket: %v", err)
			}

			key := "test-write-largefile.bin"

			// Write via backend (simulating NFSv4 write)
			start := time.Now()
			_, err = backend.PutObject(context.Background(), testBucket, key, newGeneratedPatternReader(size), "application/octet-stream")
			duration := time.Since(start)

			if err != nil {
				t.Fatalf("failed to write file: %v", err)
			}

			// Verify via direct read
			rc, _, err := backend.GetObject(context.Background(), testBucket, key)
			if err != nil {
				t.Fatalf("failed to read back file: %v", err)
			}
			defer rc.Close()

			readSize, readChecksum, err := readChecksum(rc)
			if err != nil {
				t.Fatalf("failed to verify file: %v", err)
			}

			if readSize != size {
				t.Errorf("size mismatch after write: got %d, want %d", readSize, size)
			}
			if readChecksum != checksum {
				t.Errorf("checksum mismatch after write: got %x, want %x", readChecksum, checksum)
			}

			throughput := float64(size) / duration.Seconds()
			t.Logf("Write Size: %d, Throughput: %.2f MB/s", size, throughput/(1024*1024))
		})
	}
}

func TestNFSv4ConcurrentLargeFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large file test in short mode")
	}

	backend, err := storage.NewLocalBackend(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}
	defer backend.Close()

	// Create bucket first
	err = backend.CreateBucket(context.Background(), testBucket)
	if err != nil && err != storage.ErrBucketAlreadyExists {
		t.Fatalf("failed to create bucket: %v", err)
	}

	const numConcurrent = 6
	const fileSize = 8 * 1024 * 1024
	checksum := checksumForGeneratedPattern(fileSize)

	// Launch concurrent writers
	done := make(chan int, numConcurrent)
	errors := make(chan error, numConcurrent)

	for i := 0; i < numConcurrent; i++ {
		go func(idx int) {
			key := fmt.Sprintf("test-concurrent-%d.bin", idx)
			_, err := backend.PutObject(context.Background(), testBucket, key, newGeneratedPatternReader(fileSize), "application/octet-stream")
			if err != nil {
				errors <- err
				return
			}

			// Verify
			rc, _, err := backend.GetObject(context.Background(), testBucket, key)
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

	// Wait for all goroutines
	for i := 0; i < numConcurrent; i++ {
		select {
		case <-done:
			// Success
		case err := <-errors:
			t.Fatalf("concurrent transfer failed: %v", err)
		}
	}
}

func TestNFSv4BufferPoolNoLeaks(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large file test in short mode")
	}

	backend, err := storage.NewLocalBackend(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}
	defer backend.Close()

	// Create bucket first
	err = backend.CreateBucket(context.Background(), testBucket)
	if err != nil && err != storage.ErrBucketAlreadyExists {
		t.Fatalf("failed to create bucket: %v", err)
	}

	// Cover all buffer tiers without repeating large object I/O.
	sizes := []int64{
		100 * 1024,       // 100KB (small)
		2 * 1024 * 1024,  // 2MB (medium)
		10*1024*1024 + 1, // just over the large-buffer threshold
	}

	for i, size := range sizes {
		key := fmt.Sprintf("test-leak-%d.bin", i)

		_, err := backend.PutObject(context.Background(), testBucket, key, newGeneratedPatternReader(size), "application/octet-stream")
		if err != nil {
			t.Fatalf("failed to write file %d: %v", i, err)
		}

		rc, _, err := backend.GetObject(context.Background(), testBucket, key)
		if err != nil {
			t.Fatalf("failed to read file %d: %v", i, err)
		}
		rc.Close()
	}

	// Test passes if no panic or OOM occurs
}
