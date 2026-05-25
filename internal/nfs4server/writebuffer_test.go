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
