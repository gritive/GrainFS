package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

type countingReadAtBackend struct {
	storage.Backend
	readAtCalls  atomic.Int32
	getObjCalls  atomic.Int32
	lastOffset   atomic.Int64
	lastReadSize atomic.Int64
}

func (b *countingReadAtBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	b.getObjCalls.Add(1)
	return b.Backend.GetObject(ctx, bucket, key)
}

func (b *countingReadAtBackend) ReadAt(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, error) {
	b.readAtCalls.Add(1)
	b.lastOffset.Store(offset)
	b.lastReadSize.Store(int64(len(buf)))
	return b.Backend.(interface {
		ReadAt(context.Context, string, string, int64, []byte) (int, error)
	}).ReadAt(ctx, bucket, key, offset, buf)
}

type transientNotFoundBackend struct {
	storage.Backend
	getFailures  atomic.Int32
	headFailures atomic.Int32
}

func (b *transientNotFoundBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	if b.getFailures.Add(1) == 1 {
		return nil, nil, storage.ErrObjectNotFound
	}
	return b.Backend.GetObject(ctx, bucket, key)
}

func (b *transientNotFoundBackend) HeadObject(ctx context.Context, bucket, key string) (*storage.Object, error) {
	if b.headFailures.Add(1) == 1 {
		return nil, storage.ErrObjectNotFound
	}
	return b.Backend.HeadObject(ctx, bucket, key)
}

func TestGetAndHeadObjectRetryTransientReadAfterWriteNotFound(t *testing.T) {
	tmp := t.TempDir()
	local, err := storage.NewLocalBackend(tmp)
	require.NoError(t, err)
	require.NoError(t, local.CreateBucket(context.Background(), "b"))
	_, err = local.PutObject(context.Background(), "b", "obj", bytes.NewReader([]byte("body")), "text/plain")
	require.NoError(t, err)
	require.NoError(t, local.SetObjectACL("b", "obj", 1)) // ACLPublicRead

	backend := &transientNotFoundBackend{Backend: local}
	port := freePort(t)
	s := New(fmt.Sprintf("127.0.0.1:%d", port), backend)
	go s.Run()
	t.Cleanup(func() {
		_ = s.Shutdown(context.Background())
	})
	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/b/obj", port))
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, []byte("body"), body)
	require.GreaterOrEqual(t, backend.getFailures.Load(), int32(2))

	req, err := http.NewRequest(http.MethodHead, fmt.Sprintf("http://127.0.0.1:%d/b/obj", port), nil)
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.GreaterOrEqual(t, backend.headFailures.Load(), int32(2))
}

func TestGetObjectRange_UsesBackendReadAtWhenAvailable(t *testing.T) {
	tmp := t.TempDir()
	local, err := storage.NewLocalBackend(tmp)
	require.NoError(t, err)
	require.NoError(t, local.CreateBucket(context.Background(), "b"))

	payload := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	_, err = local.PutObject(context.Background(), "b", "obj", bytes.NewReader(payload), "application/octet-stream")
	require.NoError(t, err)
	require.NoError(t, local.SetObjectACL("b", "obj", 1)) // ACLPublicRead

	backend := &countingReadAtBackend{Backend: local}
	port := freePort(t)
	s := New(fmt.Sprintf("127.0.0.1:%d", port), backend)
	go s.Run()
	t.Cleanup(func() {
		_ = s.Shutdown(context.Background())
	})
	time.Sleep(100 * time.Millisecond)

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/b/obj", port), nil)
	require.NoError(t, err)
	req.Header.Set("Range", "bytes=10-19")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusPartialContent, resp.StatusCode)
	require.Equal(t, "bytes 10-19/36", resp.Header.Get("Content-Range"))
	require.Equal(t, payload[10:20], body)
	require.Equal(t, int32(1), backend.readAtCalls.Load())
	require.Equal(t, int64(10), backend.lastOffset.Load())
	require.Equal(t, int64(10), backend.lastReadSize.Load())
	require.Zero(t, backend.getObjCalls.Load())
}

func TestGetObjectRange_LargeRangeDoesNotAllocateFullBody(t *testing.T) {
	tmp := t.TempDir()
	backend, err := storage.NewLocalBackend(tmp)
	require.NoError(t, err)
	require.NoError(t, backend.CreateBucket(context.Background(), "b"))

	payload := bytes.Repeat([]byte("x"), 32<<20)
	_, err = backend.PutObject(context.Background(), "b", "large.bin", bytes.NewReader(payload), "application/octet-stream")
	require.NoError(t, err)
	require.NoError(t, backend.SetObjectACL("b", "large.bin", 1)) // ACLPublicRead

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
