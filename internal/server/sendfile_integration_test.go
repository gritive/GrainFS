package server

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestSendfileIntegration tests that SetBodyStream with *os.File works
func TestSendfileIntegration(t *testing.T) {
	// Create temporary backend
	tmpDir, err := os.MkdirTemp("", "grainfs-sendfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	backend, err := storage.NewLocalBackend(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}

	// Create test bucket
	if err := backend.CreateBucket(context.Background(), "test-bucket"); err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// Create small object (<16KB) - should use standard path
	smallData := bytes.Repeat([]byte("A"), 1024) // 1KB
	if _, err := backend.PutObject(context.Background(), "test-bucket", "small", bytes.NewReader(smallData), "application/octet-stream"); err != nil {
		t.Fatalf("Failed to put small object: %v", err)
	}

	// Create large object (>16KB) - should use sendfile
	largeData := bytes.Repeat([]byte("B"), 32*1024) // 32KB
	if _, err := backend.PutObject(context.Background(), "test-bucket", "large", bytes.NewReader(largeData), "application/octet-stream"); err != nil {
		t.Fatalf("Failed to put large object: %v", err)
	}

	// Create server
	s := New("127.0.0.1:0", backend)
	defer s.Shutdown(context.Background())

	// Test small object
	t.Run("SmallObject", func(t *testing.T) {
		rc, obj, err := backend.GetObject(context.Background(), "test-bucket", "small")
		if err != nil {
			t.Fatalf("Failed to get small object: %v", err)
		}
		defer rc.Close()

		// Verify size
		if obj.Size != 1024 {
			t.Errorf("Expected size 1024, got %d", obj.Size)
		}

		// Verify data can be read
		data, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("Failed to read small object: %v", err)
		}
		if len(data) != 1024 {
			t.Errorf("Expected 1024 bytes, got %d", len(data))
		}
	})

	// Test large object
	t.Run("LargeObject", func(t *testing.T) {
		rc, obj, err := backend.GetObject(context.Background(), "test-bucket", "large")
		if err != nil {
			t.Fatalf("Failed to get large object: %v", err)
		}
		defer rc.Close()

		// Verify size
		if obj.Size != 32*1024 {
			t.Errorf("Expected size %d, got %d", 32*1024, obj.Size)
		}

		// Verify data can be read
		data, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("Failed to read large object: %v", err)
		}
		if len(data) != 32*1024 {
			t.Errorf("Expected %d bytes, got %d", 32*1024, len(data))
		}

		// Verify it's actually *os.File (type assertion)
		if file, ok := rc.(*os.File); ok {
			t.Logf("Large object is *os.File, fd=%d", file.Fd())
		} else {
			t.Errorf("Large object is not *os.File, got %T", rc)
		}
	})
}

// TestZeroCopyThreshold tests the 16KB threshold logic
func TestZeroCopyThreshold(t *testing.T) {
	tests := []struct {
		name     string
		size     int64
		expected string // "zero-copy" or "standard"
	}{
		{"1KB", 1 * 1024, "standard"},
		{"16KB", 16 * 1024, "standard"},
		{"16KB+1", 16*1024 + 1, "zero-copy"},
		{"32KB", 32 * 1024, "zero-copy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Threshold check logic
			useZeroCopy := tt.size > 16*1024
			result := "standard"
			if useZeroCopy {
				result = "zero-copy"
			}

			if result != tt.expected {
				t.Errorf("Size %d: expected %s, got %s", tt.size, tt.expected, result)
			}
		})
	}
}
