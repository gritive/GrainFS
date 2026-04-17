package pullthrough_test

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/pullthrough"
)

// stubUpstream simulates an upstream S3-compatible source.
type stubUpstream struct {
	objects map[string]string // "bucket/key" -> content
}

func (u *stubUpstream) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	content, ok := u.objects[bucket+"/"+key]
	if !ok {
		return nil, nil, storage.ErrObjectNotFound
	}
	return io.NopCloser(strings.NewReader(content)), &storage.Object{
		Key:  key,
		Size: int64(len(content)),
		ETag: "upstream-etag",
	}, nil
}

func newLocalBackend(t *testing.T) storage.Backend {
	t.Helper()
	b, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	return b
}

func TestPullThrough_GetObject_LocalHit(t *testing.T) {
	local := newLocalBackend(t)
	upstream := &stubUpstream{objects: map[string]string{"b/k": "upstream-data"}}

	require.NoError(t, local.CreateBucket("b"))
	_, err := local.PutObject("b", "k", strings.NewReader("local-data"), "text/plain")
	require.NoError(t, err)

	pt := pullthrough.NewBackend(local, upstream)
	rc, obj, err := pt.GetObject("b", "k")
	require.NoError(t, err)
	defer rc.Close()

	body, _ := io.ReadAll(rc)
	assert.Equal(t, "local-data", string(body), "local hit must return local content")
	assert.Equal(t, "k", obj.Key)
}

func TestPullThrough_GetObject_FetchFromUpstream(t *testing.T) {
	local := newLocalBackend(t)
	upstream := &stubUpstream{objects: map[string]string{"b/img.png": "upstream-bytes"}}

	require.NoError(t, local.CreateBucket("b"))

	pt := pullthrough.NewBackend(local, upstream)
	rc, obj, err := pt.GetObject("b", "img.png")
	require.NoError(t, err, "must fetch from upstream on cache miss")
	defer rc.Close()

	body, _ := io.ReadAll(rc)
	assert.Equal(t, "upstream-bytes", string(body), "cache miss must return upstream content")
	assert.NotNil(t, obj)

	// Second call should be a local hit now
	rc2, _, err := local.GetObject("b", "img.png")
	require.NoError(t, err, "object must be cached locally after pull-through")
	defer rc2.Close()
	body2, _ := io.ReadAll(rc2)
	assert.Equal(t, "upstream-bytes", string(body2), "cached copy must match upstream content")
}

func TestPullThrough_GetObject_NotFound(t *testing.T) {
	local := newLocalBackend(t)
	upstream := &stubUpstream{objects: map[string]string{}}

	require.NoError(t, local.CreateBucket("b"))

	pt := pullthrough.NewBackend(local, upstream)
	_, _, err := pt.GetObject("b", "missing.txt")
	assert.ErrorIs(t, err, storage.ErrObjectNotFound,
		"must return ErrObjectNotFound when neither local nor upstream has the object")
}

func TestPullThrough_PutObject_GoesToLocal(t *testing.T) {
	local := newLocalBackend(t)
	upstream := &stubUpstream{objects: map[string]string{}}

	require.NoError(t, local.CreateBucket("b"))

	pt := pullthrough.NewBackend(local, upstream)
	_, err := pt.PutObject("b", "new.txt", strings.NewReader("new"), "text/plain")
	require.NoError(t, err)

	// Must be in local
	rc, _, err := local.GetObject("b", "new.txt")
	require.NoError(t, err)
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	assert.Equal(t, "new", string(body))
}
