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

// TestGetObject_IfNoneMatch tests that GET returns 304 when ETag matches
func TestGetObject_IfNoneMatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "grainfs-etag-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	backend, err := storage.NewLocalBackend(tmpDir)
	require.NoError(t, err)
	require.NoError(t, backend.CreateBucket(context.Background(), "test-bucket"))

	data := bytes.Repeat([]byte("X"), 1024)
	obj, err := backend.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(data), "text/plain")
	require.NoError(t, err)

	s := New("127.0.0.1:14859", backend)
	go func() { s.Run() }()
	defer s.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	etag := fmt.Sprintf("%q", obj.ETag)

	t.Run("matching ETag returns 304", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "http://127.0.0.1:14859/test-bucket/file.txt", nil)
		req.Header.Set("If-None-Match", etag)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusNotModified, resp.StatusCode, "should return 304 for matching ETag")

		body, _ := io.ReadAll(resp.Body)
		require.Empty(t, body, "304 response must have no body")
	})

	t.Run("non-matching ETag returns 200", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "http://127.0.0.1:14859/test-bucket/file.txt", nil)
		req.Header.Set("If-None-Match", `"stale-etag"`)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode, "should return 200 for non-matching ETag")
	})

	t.Run("wildcard If-None-Match returns 304", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "http://127.0.0.1:14859/test-bucket/file.txt", nil)
		req.Header.Set("If-None-Match", "*")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusNotModified, resp.StatusCode, "wildcard should match any existing ETag")
	})
}

// TestGetObject_IfModifiedSince tests that GET returns 304 when not modified since
func TestGetObject_IfModifiedSince(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "grainfs-ims-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	backend, err := storage.NewLocalBackend(tmpDir)
	require.NoError(t, err)
	require.NoError(t, backend.CreateBucket(context.Background(), "test-bucket"))

	data := bytes.Repeat([]byte("Y"), 512)
	_, err = backend.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(data), "text/plain")
	require.NoError(t, err)

	s := New("127.0.0.1:14861", backend)
	go func() { s.Run() }()
	defer s.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	t.Run("future date returns 304", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "http://127.0.0.1:14861/test-bucket/file.txt", nil)
		// Set If-Modified-Since to a future date - object not modified since then
		req.Header.Set("If-Modified-Since", time.Now().Add(time.Hour).UTC().Format(http.TimeFormat))

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusNotModified, resp.StatusCode, "not modified since future date should return 304")
		body, _ := io.ReadAll(resp.Body)
		require.Empty(t, body, "304 must have no body")
	})

	t.Run("past date returns 200", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "http://127.0.0.1:14861/test-bucket/file.txt", nil)
		// Set If-Modified-Since to past date - object IS modified since then
		req.Header.Set("If-Modified-Since", time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat))

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode, "modified since past date should return 200")
	})
}

// TestGetObject_IfMatch tests that GET returns 412 when ETag does not match If-Match
func TestGetObject_IfMatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "grainfs-ifmatch-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	backend, err := storage.NewLocalBackend(tmpDir)
	require.NoError(t, err)
	require.NoError(t, backend.CreateBucket(context.Background(), "test-bucket"))

	data := bytes.Repeat([]byte("M"), 512)
	obj, err := backend.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(data), "text/plain")
	require.NoError(t, err)

	s := New("127.0.0.1:14862", backend)
	go func() { s.Run() }()
	defer s.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	etag := fmt.Sprintf("%q", obj.ETag)

	t.Run("matching ETag returns 200", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "http://127.0.0.1:14862/test-bucket/file.txt", nil)
		req.Header.Set("If-Match", etag)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode, "matching ETag should return 200")
	})

	t.Run("non-matching ETag returns 412", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "http://127.0.0.1:14862/test-bucket/file.txt", nil)
		req.Header.Set("If-Match", `"wrong-etag"`)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusPreconditionFailed, resp.StatusCode, "non-matching ETag should return 412")
	})
}

// TestHeadObject_ConditionalHeaders tests that HEAD also respects If-None-Match
func TestHeadObject_ConditionalHeaders(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "grainfs-head-cond-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	backend, err := storage.NewLocalBackend(tmpDir)
	require.NoError(t, err)
	require.NoError(t, backend.CreateBucket(context.Background(), "test-bucket"))

	data := bytes.Repeat([]byte("H"), 256)
	obj, err := backend.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(data), "text/plain")
	require.NoError(t, err)

	s := New("127.0.0.1:14863", backend)
	go func() { s.Run() }()
	defer s.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	etag := fmt.Sprintf("%q", obj.ETag)

	t.Run("HEAD with matching If-None-Match returns 304", func(t *testing.T) {
		req, _ := http.NewRequest("HEAD", "http://127.0.0.1:14863/test-bucket/file.txt", nil)
		req.Header.Set("If-None-Match", etag)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusNotModified, resp.StatusCode)
	})
}

func TestParseByteRange(t *testing.T) {
	const size = int64(65536)

	tests := []struct {
		name      string
		header    string
		wantStart int64
		wantEnd   int64
		wantOK    bool
	}{
		{"basic range", "bytes=0-1023", 0, 1023, true},
		{"mid range", "bytes=1024-2047", 1024, 2047, true},
		{"single byte", "bytes=0-0", 0, 0, true},
		{"open end (resume)", "bytes=1024-", 1024, size - 1, true},
		{"suffix 500 bytes", "bytes=-500", size - 500, size - 1, true},
		{"clamped end", "bytes=0-99999", 0, size - 1, true},
		{"last byte", "bytes=65535-65535", 65535, 65535, true},
		{"start at end", "bytes=65536-", 0, 0, false},   // start >= size
		{"reversed range", "bytes=100-50", 0, 0, false}, // end < start
		{"invalid format", "bytes=abc", 0, 0, false},
		{"no bytes prefix", "tokens=0-100", 0, 0, false},
		{"suffix zero", "bytes=-0", 0, 0, false},
		{"start equals size", "bytes=65536-65537", 0, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, ok := parseByteRange(tt.header, size)
			require.Equal(t, tt.wantOK, ok, "ok mismatch")
			if ok {
				require.Equal(t, tt.wantStart, start, "start mismatch")
				require.Equal(t, tt.wantEnd, end, "end mismatch")
			}
		})
	}
}
