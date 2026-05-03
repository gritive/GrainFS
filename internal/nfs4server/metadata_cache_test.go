package nfs4server

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadFileMeta_CachesMissingSidecar(t *testing.T) {
	backend := &fileMetaCacheBackend{}
	d := &Dispatcher{backend: backend, state: NewStateManager()}

	first := d.loadFileMeta("hot.bin")
	second := d.loadFileMeta("hot.bin")

	require.Equal(t, uint32(0644), first.Mode)
	require.Equal(t, first, second)
	assert.Equal(t, 1, backend.getCalls, "missing sidecar should be cached after first lookup")
}

type fileMetaCacheBackend struct {
	getCalls int
}

func (b *fileMetaCacheBackend) CreateBucket(context.Context, string) error { return nil }
func (b *fileMetaCacheBackend) HeadBucket(context.Context, string) error   { return nil }
func (b *fileMetaCacheBackend) DeleteBucket(context.Context, string) error { return nil }
func (b *fileMetaCacheBackend) ListBuckets(context.Context) ([]string, error) {
	return nil, nil
}
func (b *fileMetaCacheBackend) PutObject(context.Context, string, string, io.Reader, string) (*storage.Object, error) {
	return &storage.Object{}, nil
}
func (b *fileMetaCacheBackend) GetObject(context.Context, string, string) (io.ReadCloser, *storage.Object, error) {
	b.getCalls++
	return nil, nil, errors.New("missing sidecar")
}
func (b *fileMetaCacheBackend) HeadObject(context.Context, string, string) (*storage.Object, error) {
	return &storage.Object{}, nil
}
func (b *fileMetaCacheBackend) DeleteObject(context.Context, string, string) error { return nil }
func (b *fileMetaCacheBackend) ListObjects(context.Context, string, string, int) ([]*storage.Object, error) {
	return nil, nil
}
func (b *fileMetaCacheBackend) WalkObjects(context.Context, string, string, func(*storage.Object) error) error {
	return nil
}
func (b *fileMetaCacheBackend) CreateMultipartUpload(context.Context, string, string, string) (*storage.MultipartUpload, error) {
	return nil, nil
}
func (b *fileMetaCacheBackend) UploadPart(context.Context, string, string, string, int, io.Reader) (*storage.Part, error) {
	return nil, nil
}
func (b *fileMetaCacheBackend) CompleteMultipartUpload(context.Context, string, string, string, []storage.Part) (*storage.Object, error) {
	return nil, nil
}
func (b *fileMetaCacheBackend) AbortMultipartUpload(context.Context, string, string, string) error {
	return nil
}
