package migration_test

import (
	"io"
	"strings"
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

func (s *stubSource) ListObjects(bucket string) ([]string, error) {
	var keys []string
	for _, o := range s.objects[bucket] {
		keys = append(keys, o.key)
	}
	return keys, nil
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

func newLocalBackend(t *testing.T) storage.Backend {
	t.Helper()
	b, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	return b
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
	dst := newLocalBackend(t)

	inj := migration.NewInjector(src, dst)
	stats, err := inj.Run()
	require.NoError(t, err)
	assert.Equal(t, 2, stats.Copied)
	assert.Equal(t, 0, stats.Errors)

	// Verify objects in destination
	rc, _, err := dst.GetObject("bucket1", "file.txt")
	require.NoError(t, err)
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	assert.Equal(t, "hello", string(body))

	rc2, _, err := dst.GetObject("bucket1", "img/photo.jpg")
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
	dst := newLocalBackend(t)
	require.NoError(t, dst.CreateBucket("b"))
	_, err := dst.PutObject("b", "existing.txt", strings.NewReader("dst"), "text/plain")
	require.NoError(t, err)

	inj := migration.NewInjector(src, dst, migration.WithSkipExisting(true))
	stats, err := inj.Run()
	require.NoError(t, err)
	assert.Equal(t, 0, stats.Copied)
	assert.Equal(t, 1, stats.Skipped)

	// Destination content must remain unchanged
	rc, _, err := dst.GetObject("b", "existing.txt")
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
	dst := newLocalBackend(t)

	inj := migration.NewInjector(src, dst)
	stats, err := inj.Run()
	require.NoError(t, err)
	assert.Equal(t, 2, stats.Copied)

	for _, tc := range []struct{ bucket, key, want string }{
		{"alpha", "a.txt", "aaa"},
		{"beta", "b.txt", "bbb"},
	} {
		rc, _, err := dst.GetObject(tc.bucket, tc.key)
		require.NoError(t, err, tc.key)
		b, _ := io.ReadAll(rc)
		rc.Close()
		assert.Equal(t, tc.want, string(b))
	}
}
