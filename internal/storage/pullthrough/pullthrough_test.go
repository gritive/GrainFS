package pullthrough_test

import (
	"bytes"
	"errors"
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

// streamingUpstream returns a ReadCloser that streams arbitrary bytes without
// buffering the full body up-front. Used to verify pullthrough does not
// buffer the entire upstream body in memory.
type streamingUpstream struct {
	content io.Reader
	size    int64
}

func (u *streamingUpstream) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	return io.NopCloser(u.content), &storage.Object{
		Key:  key,
		Size: u.size,
		ETag: "streaming-etag",
	}, nil
}

// TestPullthrough_LargeObject_Streaming verifies that a large upstream object
// is streamed through pullthrough without buffering the full body.
// Regression for Known Issue #1: io.ReadAll previously buffered the entire
// object in memory before writing locally (OOM risk).
func TestPullthrough_LargeObject_Streaming(t *testing.T) {
	local := newLocalBackend(t)
	require.NoError(t, local.CreateBucket("b"))

	// 10MB of zeroes via streaming reader (no buffer materialization)
	const size = 10 * 1024 * 1024
	content := io.LimitReader(zeroReader{}, size)
	upstream := &streamingUpstream{content: content, size: size}

	pt := pullthrough.NewBackend(local, upstream)
	rc, obj, err := pt.GetObject("b", "big.bin")
	require.NoError(t, err)
	defer rc.Close()

	// Caller must receive full body bytes
	n, err := io.Copy(io.Discard, rc)
	require.NoError(t, err)
	assert.Equal(t, int64(size), n, "caller must receive full body")
	assert.Equal(t, int64(size), obj.Size, "object metadata must report full size")

	// Verify the body was cached locally
	rc2, _, err := local.GetObject("b", "big.bin")
	require.NoError(t, err, "object must be cached locally after streaming pull")
	defer rc2.Close()
	n2, err := io.Copy(io.Discard, rc2)
	require.NoError(t, err)
	assert.Equal(t, int64(size), n2, "cached copy must have full size")
}

// TestPullthrough_CallerReceivesFullBody is a regression test for the
// 2-pass streaming fix: the caller MUST receive the full body bytes.
// In the naive io.Pipe+Discard approach, the caller would get an empty body.
func TestPullthrough_CallerReceivesFullBody(t *testing.T) {
	local := newLocalBackend(t)
	require.NoError(t, local.CreateBucket("b"))

	payload := bytes.Repeat([]byte("grainfs"), 1000) // 7000 bytes
	upstream := &stubUpstream{objects: map[string]string{"b/x": string(payload)}}

	pt := pullthrough.NewBackend(local, upstream)
	rc, _, err := pt.GetObject("b", "x")
	require.NoError(t, err)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, payload, got, "caller must receive identical bytes to upstream")
}

// errReader returns data then an error, simulating a mid-stream failure.
type errReader struct {
	data      []byte
	pos       int
	failAfter int
	err       error
}

func (r *errReader) Read(p []byte) (int, error) {
	if r.pos >= r.failAfter {
		return 0, r.err
	}
	remaining := r.failAfter - r.pos
	if len(p) > remaining {
		p = p[:remaining]
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

type errUpstream struct {
	reader io.Reader
	size   int64
}

func (u *errUpstream) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	return io.NopCloser(u.reader), &storage.Object{Key: key, Size: u.size, ETag: "err"}, nil
}

// TestPullthrough_UpstreamErrorMidStream verifies that when upstream fails
// mid-stream, the caller gets an error and no corrupt entry is left in cache.
func TestPullthrough_UpstreamErrorMidStream(t *testing.T) {
	local := newLocalBackend(t)
	require.NoError(t, local.CreateBucket("b"))

	payload := bytes.Repeat([]byte("x"), 1000)
	upstream := &errUpstream{
		reader: &errReader{data: payload, failAfter: 500, err: errors.New("upstream broke")},
		size:   1000,
	}

	pt := pullthrough.NewBackend(local, upstream)
	rc, _, err := pt.GetObject("b", "bad")
	// Must propagate the error (either from pull or post-read)
	if err == nil {
		defer rc.Close()
		_, err = io.ReadAll(rc)
	}
	require.Error(t, err, "upstream error must surface to caller")

	// Local cache must NOT contain a partial entry
	_, _, err = local.GetObject("b", "bad")
	assert.ErrorIs(t, err, storage.ErrObjectNotFound,
		"partial upstream stream must not create a local cache entry")
}

// zeroReader returns an infinite stream of zero bytes.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
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
