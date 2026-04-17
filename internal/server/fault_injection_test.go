package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// errorBackend wraps a real backend and injects errors on GetObject/HeadObject.
type errorBackend struct {
	storage.Backend
	getErr  error
	headErr error
}

func (e *errorBackend) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	if e.getErr != nil {
		return nil, nil, e.getErr
	}
	return e.Backend.GetObject(bucket, key)
}

func (e *errorBackend) HeadObject(bucket, key string) (*storage.Object, error) {
	if e.headErr != nil {
		return nil, e.headErr
	}
	return e.Backend.HeadObject(bucket, key)
}

// TestGetObject_BackendError tests that a backend GetObject error returns 500
func TestGetObject_BackendError(t *testing.T) {
	tmpDir := t.TempDir()
	real, err := storage.NewLocalBackend(tmpDir)
	require.NoError(t, err)
	require.NoError(t, real.CreateBucket("test-bucket"))

	data := bytes.Repeat([]byte("X"), 1024)
	_, err = real.PutObject("test-bucket", "file.bin", bytes.NewReader(data), "application/octet-stream")
	require.NoError(t, err)

	s := New("127.0.0.1:14870", &errorBackend{Backend: real, getErr: errors.New("disk failure")})
	go func() { s.Run() }()
	defer s.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get("http://127.0.0.1:14870/test-bucket/file.bin")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

// TestHeadObject_BackendError tests that a backend HeadObject error returns 500
func TestHeadObject_BackendError(t *testing.T) {
	tmpDir := t.TempDir()
	real, err := storage.NewLocalBackend(tmpDir)
	require.NoError(t, err)
	require.NoError(t, real.CreateBucket("test-bucket"))

	_, err = real.PutObject("test-bucket", "file.bin", bytes.NewReader([]byte("hello")), "text/plain")
	require.NoError(t, err)

	s := New("127.0.0.1:14871", &errorBackend{Backend: real, headErr: errors.New("disk failure")})
	go func() { s.Run() }()
	defer s.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	req, _ := http.NewRequest("HEAD", "http://127.0.0.1:14871/test-bucket/file.bin", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

// TestGetObject_SmallFilePartialReadReturns500 tests that a mid-stream error on a small file
// (which uses io.ReadAll before sending) returns 500, not a partial 200.
func TestGetObject_SmallFilePartialReadReturns500(t *testing.T) {
	tmpDir := t.TempDir()
	real, err := storage.NewLocalBackend(tmpDir)
	require.NoError(t, err)
	require.NoError(t, real.CreateBucket("test-bucket"))

	// 8KB: below the 16KB zero-copy threshold → uses io.ReadAll path
	data := bytes.Repeat([]byte("S"), 8*1024)
	_, err = real.PutObject("test-bucket", "small.bin", bytes.NewReader(data), "application/octet-stream")
	require.NoError(t, err)

	s := New("127.0.0.1:14872", &partialErrorBackend{Backend: real, failAfter: 512})
	go func() { s.Run() }()
	defer s.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get("http://127.0.0.1:14872/test-bucket/small.bin")
	require.NoError(t, err)
	defer resp.Body.Close()

	// Small file: io.ReadAll is called first, error happens before headers sent → must return 500
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode,
		"partial read on small file should return 500 (not partial 200)")
}

// TestGetObject_LargeFilePartialReadTruncates tests that a mid-stream error on a large file
// (which uses SetBodyStream / zero-copy) results in a truncated response.
// Once headers are sent, we cannot change the status code, so the connection closes early.
func TestGetObject_LargeFilePartialReadTruncates(t *testing.T) {
	tmpDir := t.TempDir()
	real, err := storage.NewLocalBackend(tmpDir)
	require.NoError(t, err)
	require.NoError(t, real.CreateBucket("test-bucket"))

	// 64KB: above the 16KB threshold → uses SetBodyStream (zero-copy)
	data := bytes.Repeat([]byte("L"), 64*1024)
	_, err = real.PutObject("test-bucket", "large.bin", bytes.NewReader(data), "application/octet-stream")
	require.NoError(t, err)

	s := New("127.0.0.1:14873", &partialErrorBackend{Backend: real, failAfter: 1024})
	go func() { s.Run() }()
	defer s.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get("http://127.0.0.1:14873/test-bucket/large.bin")
	require.NoError(t, err)
	defer resp.Body.Close()

	// Large file: headers (200 OK) sent before body streaming — status is 200
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Body is truncated: client receives < full file
	body, _ := io.ReadAll(resp.Body)
	require.Less(t, len(body), len(data), "partial read should truncate the body")
}

// TestColdDataIntegrity tests that objects are retrieved correctly on first access (no warm cache).
func TestColdDataIntegrity(t *testing.T) {
	tmpDir := t.TempDir()
	backend, err := storage.NewLocalBackend(tmpDir)
	require.NoError(t, err)
	require.NoError(t, backend.CreateBucket("test-bucket"))

	s := New("127.0.0.1:14874", backend)
	go func() { s.Run() }()
	defer s.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	sizes := []int{
		512,         // small: standard path
		16 * 1024,   // exactly at threshold
		16*1024 + 1, // one byte over threshold: zero-copy path
		256 * 1024,  // large: zero-copy
	}

	for _, size := range sizes {
		t.Run(byteLabel(size), func(t *testing.T) {
			key := byteLabel(size)
			original := bytes.Repeat([]byte{byte(size & 0xff)}, size)

			_, err := backend.PutObject("test-bucket", key, bytes.NewReader(original), "application/octet-stream")
			require.NoError(t, err)

			resp, err := http.Get("http://127.0.0.1:14874/test-bucket/" + key)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)

			got, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, original, got, "cold data integrity mismatch for size %d", size)
		})
	}
}

// partialErrorBackend returns a reader that fails after failAfter bytes.
type partialErrorBackend struct {
	storage.Backend
	failAfter int
}

func (p *partialErrorBackend) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	rc, obj, err := p.Backend.GetObject(bucket, key)
	if err != nil {
		return nil, nil, err
	}
	return &limitedErrReader{ReadCloser: rc, remaining: p.failAfter}, obj, nil
}

type limitedErrReader struct {
	io.ReadCloser
	remaining int
}

func (r *limitedErrReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, errors.New("simulated partial read failure")
	}
	if len(p) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.ReadCloser.Read(p)
	r.remaining -= n
	return n, err
}

func byteLabel(n int) string {
	switch {
	case n < 1024:
		return fmt.Sprintf("%dB", n)
	case n < 1024*1024:
		return fmt.Sprintf("%dKiB", n/1024)
	default:
		return fmt.Sprintf("%dMiB", n/1024/1024)
	}
}
