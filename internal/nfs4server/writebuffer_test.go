package nfs4server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestWriteBuffer_FirstWriteCreatesFile(t *testing.T) {
	dir := t.TempDir()
	wb := newWriteBuffer(dir, &fakeBackend{})
	require.NoError(t, wb.Write(context.Background(), "bkt", "key", 0, []byte("hello"), "text/plain"))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(entries), 1, "buffer file should be created on first write")
	// Locate the data file (not the .meta sidecar).
	var dataPath string
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".meta" {
			dataPath = filepath.Join(dir, e.Name())
			break
		}
	}
	require.NotEmpty(t, dataPath)
	got, err := os.ReadFile(dataPath)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), got)
}

// Must return storage.ErrObjectNotFound (not a generic error) so the
// production code's errors.Is check in materializeLocked treats it as
// "new object" rather than "transient backend error".
type fakeBackend struct {
	mu          sync.Mutex
	PutCalls    int
	LastPutBody []byte
	GetReturns  map[string][]byte // key="bucket/key" → bytes; missing = NotFound
	PutFail     bool              // when true, PutObject returns error
}

func (f *fakeBackend) HeadObject(_ context.Context, bucket, key string) (*storage.Object, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if data, ok := f.GetReturns[bucket+"/"+key]; ok {
		return &storage.Object{Size: int64(len(data))}, nil
	}
	return nil, storage.ErrObjectNotFound
}
func (f *fakeBackend) GetObject(_ context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.GetReturns[bucket+"/"+key]
	if !ok {
		return nil, nil, storage.ErrObjectNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), &storage.Object{Size: int64(len(data))}, nil
}
func (f *fakeBackend) PutObject(_ context.Context, bucket, key string, body io.Reader, _ string) (*storage.Object, error) {
	buf, _ := io.ReadAll(body)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.PutFail {
		return nil, fmt.Errorf("put fail")
	}
	f.PutCalls++
	f.LastPutBody = append([]byte(nil), buf...)
	if f.GetReturns == nil {
		f.GetReturns = map[string][]byte{}
	}
	f.GetReturns[bucket+"/"+key] = append([]byte(nil), buf...)
	return &storage.Object{Size: int64(len(buf))}, nil
}

func TestWriteBuffer_TransientBackendErrorFailsWrite(t *testing.T) {
	dir := t.TempDir()
	be := &transientErrBackend{}
	wb := newWriteBuffer(dir, be)
	err := wb.Write(context.Background(), "bkt", "key", 0, []byte("hello"), "text/plain")
	require.Error(t, err, "transient backend error must fail Write, not silently start with empty buffer")
	require.NotErrorIs(t, err, storage.ErrObjectNotFound, "must propagate the original transient error, not collapse to NotFound")
	// Buffer file must NOT have been created (would cause data loss on next flush).
	entries, _ := os.ReadDir(dir)
	require.Empty(t, entries, "no buffer file should exist when materialize failed")
}

// transientErrBackend returns a non-NotFound error from GetObject (simulating
// cluster joining, network blip). HeadObject + PutObject are unused for this
// test but defined so the small interface is satisfied.
type transientErrBackend struct{}

func (transientErrBackend) HeadObject(context.Context, string, string) (*storage.Object, error) {
	return nil, fmt.Errorf("transient: cluster not ready")
}
func (transientErrBackend) GetObject(context.Context, string, string) (io.ReadCloser, *storage.Object, error) {
	return nil, nil, fmt.Errorf("transient: cluster not ready")
}
func (transientErrBackend) PutObject(context.Context, string, string, io.Reader, string) (*storage.Object, error) {
	return nil, fmt.Errorf("transient: cluster not ready")
}

func TestWriteBuffer_ReadAfterWrite(t *testing.T) {
	dir := t.TempDir()
	wb := newWriteBuffer(dir, &fakeBackend{})
	require.NoError(t, wb.Write(context.Background(), "bkt", "key", 0, []byte("hello world"), "text/plain"))
	got, hit, err := wb.Read(context.Background(), "bkt", "key", 6, 5)
	require.NoError(t, err)
	require.True(t, hit, "Read on buffered key must be a hit")
	require.Equal(t, []byte("world"), got)
}

func TestWriteBuffer_ReadColdMiss(t *testing.T) {
	dir := t.TempDir()
	wb := newWriteBuffer(dir, &fakeBackend{})
	_, hit, err := wb.Read(context.Background(), "bkt", "cold-key", 0, 5)
	require.NoError(t, err)
	require.False(t, hit, "Read on never-buffered key must be a miss")
}

func TestWriteBuffer_FlushSendsPutObject(t *testing.T) {
	dir := t.TempDir()
	be := &fakeBackend{}
	wb := newWriteBuffer(dir, be)
	require.NoError(t, wb.Write(context.Background(), "bkt", "key", 0, []byte("payload"), "text/plain"))
	require.NoError(t, wb.Flush(context.Background(), "bkt", "key"))

	require.Equal(t, 1, be.PutCalls, "PutObject should be called exactly once on flush")
	require.Equal(t, []byte("payload"), be.LastPutBody)

	// Buffer file removed after flush.
	entries, _ := os.ReadDir(dir)
	require.Empty(t, entries, "buffer dir should be empty after flush")
}

func TestWriteBuffer_FlushNoOpForUnknownKey(t *testing.T) {
	dir := t.TempDir()
	wb := newWriteBuffer(dir, &fakeBackend{})
	require.NoError(t, wb.Flush(context.Background(), "bkt", "missing"))
}

func TestWriteBuffer_DiscardSkipsPutObject(t *testing.T) {
	dir := t.TempDir()
	be := &fakeBackend{}
	wb := newWriteBuffer(dir, be)
	require.NoError(t, wb.Write(context.Background(), "bkt", "key", 0, []byte("payload"), "text/plain"))
	require.NoError(t, wb.Discard(context.Background(), "bkt", "key"))

	require.Equal(t, 0, be.PutCalls, "Discard must not call PutObject")
	entries, _ := os.ReadDir(dir)
	require.Empty(t, entries, "buffer dir empty after discard")
}

func TestWriteBuffer_ReadAfterFlushReturnsMissNotError(t *testing.T) {
	dir := t.TempDir()
	be := &fakeBackend{}
	wb := newWriteBuffer(dir, be)
	require.NoError(t, wb.Write(context.Background(), "bkt", "key", 0, []byte("payload"), "text/plain"))
	require.NoError(t, wb.Flush(context.Background(), "bkt", "key"))

	// After flush the entry is gone from the map — Read returns a clean miss.
	_, hit, err := wb.Read(context.Background(), "bkt", "key", 0, 7)
	require.NoError(t, err)
	require.False(t, hit, "post-flush Read must miss without error (caller falls back to backend)")
}
