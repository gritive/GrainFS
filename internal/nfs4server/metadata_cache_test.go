package nfs4server

import (
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

func (b *fileMetaCacheBackend) CreateBucket(string) error { return nil }
func (b *fileMetaCacheBackend) HeadBucket(string) error   { return nil }
func (b *fileMetaCacheBackend) DeleteBucket(string) error { return nil }
func (b *fileMetaCacheBackend) ListBuckets() ([]string, error) {
	return nil, nil
}
func (b *fileMetaCacheBackend) PutObject(string, string, io.Reader, string) (*storage.Object, error) {
	return &storage.Object{}, nil
}
func (b *fileMetaCacheBackend) GetObject(string, string) (io.ReadCloser, *storage.Object, error) {
	b.getCalls++
	return nil, nil, errors.New("missing sidecar")
}
func (b *fileMetaCacheBackend) HeadObject(string, string) (*storage.Object, error) {
	return &storage.Object{}, nil
}
func (b *fileMetaCacheBackend) DeleteObject(string, string) error { return nil }
func (b *fileMetaCacheBackend) ListObjects(string, string, int) ([]*storage.Object, error) {
	return nil, nil
}
func (b *fileMetaCacheBackend) WalkObjects(string, string, func(*storage.Object) error) error {
	return nil
}
func (b *fileMetaCacheBackend) CreateMultipartUpload(string, string, string) (*storage.MultipartUpload, error) {
	return nil, nil
}
func (b *fileMetaCacheBackend) UploadPart(string, string, string, int, io.Reader) (*storage.Part, error) {
	return nil, nil
}
func (b *fileMetaCacheBackend) CompleteMultipartUpload(string, string, string, []storage.Part) (*storage.Object, error) {
	return nil, nil
}
func (b *fileMetaCacheBackend) AbortMultipartUpload(string, string, string) error {
	return nil
}
