package migration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/migration"
	"github.com/gritive/GrainFS/internal/storage"
)

// stubSource simulates a source storage (e.g. MinIO).
type stubSource struct {
	buckets []string
	objects map[string][]stubObject
}

type stubObject struct{ key, content, ct string }

func (s *stubSource) ListBuckets() ([]string, error) { return s.buckets, nil }

func (s *stubSource) ListObjectsPage(bucket, cursor string) ([]string, string, error) {
	if cursor != "" {
		return nil, "", nil // stub delivers all objects in the first call
	}
	var keys []string
	for _, o := range s.objects[bucket] {
		keys = append(keys, o.key)
	}
	return keys, "", nil
}

func (s *stubSource) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	for _, o := range s.objects[bucket] {
		if o.key == key {
			return io.NopCloser(strings.NewReader(o.content)), &storage.Object{
				Key:         key,
				Size:        int64(len(o.content)),
				ContentType: o.ct,
			}, nil
		}
	}
	return nil, nil, storage.ErrObjectNotFound
}

// recordingDstBackend is an in-memory migration destination. The injector only
// needs CreateBucket / GetObject (skip-existing probe) / PutObject; an in-memory
// sink tests the injector's copy/skip/pagination logic without depending on a
// concrete backend's write path (the real migration target is the production
// cluster backend).
type recordingDstBackend struct {
	storage.Backend
	mu      sync.Mutex
	objects map[string][]byte
}

func newRecordingDst() *recordingDstBackend {
	return &recordingDstBackend{objects: map[string][]byte{}}
}

func (b *recordingDstBackend) CreateBucket(context.Context, string) error { return nil }

func (b *recordingDstBackend) GetObject(_ context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.objects[bucket+"/"+key]
	if !ok {
		return nil, nil, storage.ErrObjectNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), &storage.Object{Key: key, Size: int64(len(data))}, nil
}

func (b *recordingDstBackend) PutObject(_ context.Context, bucket, key string, r io.Reader, _ string) (*storage.Object, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.objects[bucket+"/"+key] = data
	b.mu.Unlock()
	return &storage.Object{Key: key, Size: int64(len(data))}, nil
}

func newTestDst(t *testing.T) storage.Backend {
	t.Helper()
	return newRecordingDst()
}

func TestInjector_CopiesAllObjects(t *testing.T) {
	src := &stubSource{
		buckets: []string{"bucket1"},
		objects: map[string][]stubObject{
			"bucket1": {
				{key: "file.txt", content: "hello", ct: "text/plain"},
				{key: "img/photo.jpg", content: "binary", ct: "image/jpeg"},
			},
		},
	}
	dst := newTestDst(t)

	inj := migration.NewInjector(src, dst)
	stats, err := inj.Run()
	require.NoError(t, err)
	assert.Equal(t, 2, stats.Copied)
	assert.Equal(t, 0, stats.Errors)

	// Verify objects in destination
	rc, _, err := dst.GetObject(context.Background(), "bucket1", "file.txt")
	require.NoError(t, err)
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	assert.Equal(t, "hello", string(body))

	rc2, _, err := dst.GetObject(context.Background(), "bucket1", "img/photo.jpg")
	require.NoError(t, err)
	defer rc2.Close()
	body2, _ := io.ReadAll(rc2)
	assert.Equal(t, "binary", string(body2))
}

func TestInjector_SkipsExistingObjects(t *testing.T) {
	src := &stubSource{
		buckets: []string{"b"},
		objects: map[string][]stubObject{
			"b": {{key: "existing.txt", content: "src", ct: "text/plain"}},
		},
	}
	dst := newTestDst(t)
	require.NoError(t, dst.CreateBucket(context.Background(), "b"))
	_, err := dst.PutObject(context.Background(), "b", "existing.txt", strings.NewReader("dst"), "text/plain")
	require.NoError(t, err)

	inj := migration.NewInjector(src, dst, migration.WithSkipExisting(true))
	stats, err := inj.Run()
	require.NoError(t, err)
	assert.Equal(t, 0, stats.Copied)
	assert.Equal(t, 1, stats.Skipped)

	// Destination content must remain unchanged
	rc, _, err := dst.GetObject(context.Background(), "b", "existing.txt")
	require.NoError(t, err)
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	assert.Equal(t, "dst", string(body))
}

func TestInjector_MultipleBuckets(t *testing.T) {
	src := &stubSource{
		buckets: []string{"alpha", "beta"},
		objects: map[string][]stubObject{
			"alpha": {{key: "a.txt", content: "aaa", ct: "text/plain"}},
			"beta":  {{key: "b.txt", content: "bbb", ct: "text/plain"}},
		},
	}
	dst := newTestDst(t)

	inj := migration.NewInjector(src, dst)
	stats, err := inj.Run()
	require.NoError(t, err)
	assert.Equal(t, 2, stats.Copied)

	for _, tc := range []struct{ bucket, key, want string }{
		{"alpha", "a.txt", "aaa"},
		{"beta", "b.txt", "bbb"},
	} {
		rc, _, err := dst.GetObject(context.Background(), tc.bucket, tc.key)
		require.NoError(t, err, tc.key)
		b, _ := io.ReadAll(rc)
		rc.Close()
		assert.Equal(t, tc.want, string(b))
	}
}

// pagedSource simulates a source with multiple pages of objects.
type pagedSource struct {
	buckets []string
	pages   map[string][][]string // bucket → []page of keys
}

func (s *pagedSource) ListBuckets() ([]string, error) { return s.buckets, nil }

func (s *pagedSource) ListObjectsPage(bucket, cursor string) ([]string, string, error) {
	pages := s.pages[bucket]
	idx := 0
	if cursor != "" {
		fmt.Sscanf(cursor, "page:%d", &idx)
	}
	if idx >= len(pages) {
		return nil, "", nil
	}
	next := ""
	if idx+1 < len(pages) {
		next = fmt.Sprintf("page:%d", idx+1)
	}
	return pages[idx], next, nil
}

func (s *pagedSource) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	return io.NopCloser(strings.NewReader("data")), &storage.Object{Key: key}, nil
}

func TestInjector_Pagination_MultiPage(t *testing.T) {
	src := &pagedSource{
		buckets: []string{"b"},
		pages: map[string][][]string{
			"b": {{"a.txt", "b.txt"}, {"c.txt"}},
		},
	}
	dst := newTestDst(t)
	inj := migration.NewInjector(src, dst)
	stats, err := inj.Run()
	require.NoError(t, err)
	assert.Equal(t, 3, stats.Copied)
}
