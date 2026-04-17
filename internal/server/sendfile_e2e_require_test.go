package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestE2EZeroCopyDataIntegrity tests that zero-copy doesn't compromise data integrity
func TestE2EZeroCopyDataIntegrity(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "grainfs-e2e-integrity-*")
	require.NoError(t, err, "Failed to create temp dir")
	defer os.RemoveAll(tmpDir)

	backend, err := storage.NewLocalBackend(tmpDir)
	require.NoError(t, err, "Failed to create backend")

	err = backend.CreateBucket("test-bucket")
	require.NoError(t, err, "Failed to create bucket")

	// Test various file sizes
	sizes := []int{
		1 * 1024,    // 1KB (small - fallback)
		16 * 1024,   // 16KB (threshold)
		16*1024 + 1, // 16KB+1 (zero-copy)
		32 * 1024,   // 32KB (zero-copy)
		64 * 1024,   // 64KB (zero-copy)
		1024 * 1024, // 1MB (zero-copy)
	}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("%dB", size), func(t *testing.T) {
			// Create test data with known checksum
			originalData := bytes.Repeat([]byte("X"), size)

			// Calculate checksum
			expectedChecksum := fmt.Sprintf("%x", simpleChecksum(originalData))

			// Upload
			key := fmt.Sprintf("test-%d", size)
			_, err := backend.PutObject("test-bucket", key, bytes.NewReader(originalData), "application/octet-stream")
			require.NoError(t, err, "Failed to put object")

			// Download
			rc, _, err := backend.GetObject("test-bucket", key)
			require.NoError(t, err, "Failed to get object")
			defer rc.Close()

			downloadedData, err := io.ReadAll(rc)
			require.NoError(t, err, "Failed to read object")

			// Verify checksum
			actualChecksum := fmt.Sprintf("%x", simpleChecksum(downloadedData))

			require.Equal(t, expectedChecksum, actualChecksum, "Checksum mismatch")
			require.Equal(t, originalData, downloadedData, "Data mismatch")

			t.Logf("✓ Data integrity verified for %d bytes (checksum: %s)", size, actualChecksum)
		})
	}
}

// TestE2EZeroCopyConcurrentAccess tests concurrent access to verify no race conditions
func TestE2EZeroCopyConcurrentAccess(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "grainfs-e2e-concurrent-*")
	require.NoError(t, err, "Failed to create temp dir")
	defer os.RemoveAll(tmpDir)

	backend, err := storage.NewLocalBackend(tmpDir)
	require.NoError(t, err, "Failed to create backend")

	err = backend.CreateBucket("test-bucket")
	require.NoError(t, err, "Failed to create bucket")

	// Create test object
	testData := bytes.Repeat([]byte("C"), 64*1024)
	_, err = backend.PutObject("test-bucket", "concurrent", bytes.NewReader(testData), "application/octet-stream")
	require.NoError(t, err, "Failed to put object")

	// Concurrent access
	numGoroutines := 100
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			rc, _, err := backend.GetObject("test-bucket", "concurrent")
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

	// Collect errors
	for i := 0; i < numGoroutines; i++ {
		err := <-errors
		require.NoError(t, err, "Goroutine %d failed", i)
	}

	t.Logf("✓ Concurrent access test passed (%d goroutines)", numGoroutines)
}

// TestE2EZeroCopyMultipleFiles tests multiple files in sequence
func TestE2EZeroCopyMultipleFiles(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "grainfs-e2e-multiple-*")
	require.NoError(t, err, "Failed to create temp dir")
	defer os.RemoveAll(tmpDir)

	backend, err := storage.NewLocalBackend(tmpDir)
	require.NoError(t, err, "Failed to create backend")

	err = backend.CreateBucket("test-bucket")
	require.NoError(t, err, "Failed to create bucket")

	// Upload multiple files
	numFiles := 50
	fileSize := 128 * 1024 // 128KB

	for i := 0; i < numFiles; i++ {
		data := bytes.Repeat([]byte(fmt.Sprintf("%d", i%10)), fileSize)
		key := fmt.Sprintf("file-%04d", i)

		_, err := backend.PutObject("test-bucket", key, bytes.NewReader(data), "application/octet-stream")
		require.NoError(t, err, "Failed to put file %s", key)
	}

	// Download all files and verify
	for i := 0; i < numFiles; i++ {
		key := fmt.Sprintf("file-%04d", i)
		expectedData := bytes.Repeat([]byte(fmt.Sprintf("%d", i%10)), fileSize)

		rc, _, err := backend.GetObject("test-bucket", key)
		require.NoError(t, err, "Failed to get file %s", key)

		data, err := io.ReadAll(rc)
		rc.Close()
		require.NoError(t, err, "Failed to read file %s", key)

		require.Equal(t, expectedData, data, "Data mismatch for file %s", key)
	}

	t.Logf("✓ Multiple files test passed (%d files)", numFiles)
}

// TestE2EZeroCopyHTTPServer tests zero-copy through actual HTTP server
func TestE2EZeroCopyHTTPServer(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "grainfs-e2e-http-*")
	require.NoError(t, err, "Failed to create temp dir")
	defer os.RemoveAll(tmpDir)

	backend, err := storage.NewLocalBackend(tmpDir)
	require.NoError(t, err, "Failed to create backend")

	err = backend.CreateBucket("test-bucket")
	require.NoError(t, err, "Failed to create bucket")

	// Create test objects (small and large)
	smallData := bytes.Repeat([]byte("S"), 1*1024)   // 1KB
	largeData := bytes.Repeat([]byte("L"), 64*1024)  // 64KB

	_, err = backend.PutObject("test-bucket", "small", bytes.NewReader(smallData), "application/octet-stream")
	require.NoError(t, err, "Failed to put small object")

	_, err = backend.PutObject("test-bucket", "large", bytes.NewReader(largeData), "application/octet-stream")
	require.NoError(t, err, "Failed to put large object")

	// Start server
	s := New("127.0.0.1:14857", backend)
	go func() {
		if err := s.Run(); err != nil && err != http.ErrServerClosed {
			t.Logf("Server error: %v", err)
		}
	}()
	defer s.Shutdown(context.Background())

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	client := &http.Client{}

	// Test small file (should use fallback path)
	t.Run("SmallFile", func(t *testing.T) {
		resp, err := client.Get("http://127.0.0.1:14857/test-bucket/small")
		require.NoError(t, err, "GET request failed")
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode, "Expected status OK")

		data, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "Failed to read response")

		require.Equal(t, smallData, data, "Data mismatch for small file")
		require.Equal(t, len(smallData), len(data), "Size mismatch for small file")

		t.Logf("✓ Small file (1KB) downloaded correctly via fallback path")
	})

	// Test large file (should use zero-copy)
	t.Run("LargeFile", func(t *testing.T) {
		resp, err := client.Get("http://127.0.0.1:14857/test-bucket/large")
		require.NoError(t, err, "GET request failed")
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode, "Expected status OK")

		data, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "Failed to read response")

		require.Equal(t, largeData, data, "Data mismatch for large file")
		require.Equal(t, len(largeData), len(data), "Size mismatch for large file")

		t.Logf("✓ Large file (64KB) downloaded correctly via zero-copy")
	})
}

// TestE2EZeroCopyRangeRequest tests that range requests return partial content (206)
func TestE2EZeroCopyRangeRequest(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "grainfs-e2e-range-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	backend, err := storage.NewLocalBackend(tmpDir)
	require.NoError(t, err)

	err = backend.CreateBucket("test-bucket")
	require.NoError(t, err)

	// 64KB object so the range is clearly within bounds
	largeData := bytes.Repeat([]byte("R"), 64*1024)
	_, err = backend.PutObject("test-bucket", "large", bytes.NewReader(largeData), "application/octet-stream")
	require.NoError(t, err)

	s := New("127.0.0.1:14858", backend)
	go func() {
		if err := s.Run(); err != nil && err != http.ErrServerClosed {
			t.Logf("Server error: %v", err)
		}
	}()
	defer s.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	t.Run("PartialContent206", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "http://127.0.0.1:14858/test-bucket/large", nil)
		req.Header.Set("Range", "bytes=0-1023")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusPartialContent, resp.StatusCode, "Expected 206 Partial Content")
		require.NotEmpty(t, resp.Header.Get("Content-Range"), "Expected Content-Range header")
		require.Equal(t, "bytes 0-1023/65536", resp.Header.Get("Content-Range"))

		data, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, 1024, len(data), "Expected 1024 bytes")
		require.Equal(t, largeData[:1024], data, "Range data must match")
	})

	t.Run("MidRange", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "http://127.0.0.1:14858/test-bucket/large", nil)
		req.Header.Set("Range", "bytes=1024-2047")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusPartialContent, resp.StatusCode)
		data, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, largeData[1024:2048], data, "Mid-range data must match")
	})
}

// TestE2EZeroCopyRangeEdgeCases tests edge cases for range requests via real server
func TestE2EZeroCopyRangeEdgeCases(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "grainfs-e2e-range-edge-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	backend, err := storage.NewLocalBackend(tmpDir)
	require.NoError(t, err)
	require.NoError(t, backend.CreateBucket("test-bucket"))

	largeData := bytes.Repeat([]byte("E"), 64*1024)
	_, err = backend.PutObject("test-bucket", "large", bytes.NewReader(largeData), "application/octet-stream")
	require.NoError(t, err)

	s := New("127.0.0.1:14860", backend)
	go func() {
		if err := s.Run(); err != nil && err != http.ErrServerClosed {
			t.Logf("Server error: %v", err)
		}
	}()
	defer s.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	t.Run("out-of-bounds start returns 416", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "http://127.0.0.1:14860/test-bucket/large", nil)
		req.Header.Set("Range", "bytes=99999-100000")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusRequestedRangeNotSatisfiable, resp.StatusCode)
		require.Contains(t, resp.Header.Get("Content-Range"), "bytes */")
	})

	t.Run("open-end range (resume)", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "http://127.0.0.1:14860/test-bucket/large", nil)
		req.Header.Set("Range", "bytes=1024-")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusPartialContent, resp.StatusCode)
		data, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, len(largeData)-1024, len(data), "open-end range should return rest of file")
		require.Equal(t, largeData[1024:], data)
	})

	t.Run("suffix range (last N bytes)", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "http://127.0.0.1:14860/test-bucket/large", nil)
		req.Header.Set("Range", "bytes=-512")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusPartialContent, resp.StatusCode)
		data, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, 512, len(data), "suffix range should return last 512 bytes")
		require.Equal(t, largeData[len(largeData)-512:], data)
	})
}

// simpleChecksum is a simple checksum for testing (not cryptographically secure)
func simpleChecksum(data []byte) byte {
	var checksum byte
	for _, b := range data {
		checksum ^= b
	}
	return checksum
}
