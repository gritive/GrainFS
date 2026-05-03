package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestGetObjectRange_LargeRangeDoesNotAllocateFullBody(t *testing.T) {
	tmp := t.TempDir()
	backend, err := storage.NewLocalBackend(tmp)
	require.NoError(t, err)
	require.NoError(t, backend.CreateBucket(context.Background(), "b"))

	payload := bytes.Repeat([]byte("x"), 32<<20)
	_, err = backend.PutObject(context.Background(), "b", "large.bin", bytes.NewReader(payload), "application/octet-stream")
	require.NoError(t, err)

	port := freePort(t)
	s := New(fmt.Sprintf("127.0.0.1:%d", port), backend)
	go s.Run()
	t.Cleanup(func() {
		_ = s.Shutdown(context.Background())
	})
	time.Sleep(100 * time.Millisecond)

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/b/large.bin", port), nil)
	require.NoError(t, err)
	req.Header.Set("Range", "bytes=0-16777215")

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	n, err := io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	require.Equal(t, http.StatusPartialContent, resp.StatusCode)
	require.Equal(t, int64(16<<20), n)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(8<<20))
}
