package server

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestGetObject_IfUnmodifiedSince tests that GET returns 412 when the object was
// modified after the If-Unmodified-Since date (precondition fails).
func TestGetObject_IfUnmodifiedSince(t *testing.T) {
	tmpDir := t.TempDir()

	backend, err := storage.NewLocalBackend(tmpDir)
	require.NoError(t, err)
	require.NoError(t, backend.CreateBucket("test-bucket"))

	data := bytes.Repeat([]byte("U"), 512)
	_, err = backend.PutObject("test-bucket", "file.txt", bytes.NewReader(data), "text/plain")
	require.NoError(t, err)

	s := New("127.0.0.1:14876", backend)
	go func() { s.Run() }()
	defer s.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	t.Run("past date returns 412 (modified since then)", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "http://127.0.0.1:14876/test-bucket/file.txt", nil)
		// Object was uploaded moments ago, so it IS modified since an hour ago → 412
		req.Header.Set("If-Unmodified-Since", time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat))

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusPreconditionFailed, resp.StatusCode,
			"object modified after If-Unmodified-Since date should return 412")
	})

	t.Run("future date returns 200 (not modified since then)", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "http://127.0.0.1:14876/test-bucket/file.txt", nil)
		// Future date → object has NOT been modified after this future date → precondition satisfied
		req.Header.Set("If-Unmodified-Since", time.Now().Add(time.Hour).UTC().Format(http.TimeFormat))

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode,
			"object not modified after If-Unmodified-Since date should return 200")
	})
}
