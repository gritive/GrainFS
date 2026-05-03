package server

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestGetObject_ContentLengthHeader verifies Content-Length is set for both
// the standard path (small files) and the zero-copy path (large files).
func TestGetObject_ContentLengthHeader(t *testing.T) {
	tmpDir := t.TempDir()
	backend, err := storage.NewLocalBackend(tmpDir)
	require.NoError(t, err)
	require.NoError(t, backend.CreateBucket(context.Background(), "test-bucket"))

	cases := []struct {
		name string
		size int
	}{
		{"small_512B", 512},
		{"small_16KiB", 16 * 1024},
		{"large_16KiBplus1", 16*1024 + 1}, // zero-copy threshold
		{"large_64KiB", 64 * 1024},
		{"large_256KiB", 256 * 1024},
	}

	for _, tc := range cases {
		data := bytes.Repeat([]byte("Z"), tc.size)
		_, err := backend.PutObject(context.Background(), "test-bucket", tc.name, bytes.NewReader(data), "application/octet-stream")
		require.NoError(t, err)
	}

	s := New("127.0.0.1:14875", backend)
	go func() { s.Run() }()
	defer s.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:14875/test-bucket/%s", tc.name))
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)

			clHeader := resp.Header.Get("Content-Length")
			require.NotEmpty(t, clHeader, "Content-Length header must be present")

			cl, err := strconv.Atoi(clHeader)
			require.NoError(t, err, "Content-Length must be a valid integer")
			require.Equal(t, tc.size, cl, "Content-Length must match actual object size")
		})
	}
}
