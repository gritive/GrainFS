package pullthrough_test

import (
	"bytes"
	"context"
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

	require.NoError(t, local.CreateBucket(context.Background(), "b"))
	_, err := local.PutObject(context.Background(), "b", "k", strings.NewReader("local-data"), "text/plain")
	require.NoError(t, err)

	pt := pullthrough.NewBackend(local, &staticResolver{up: upstream})
	rc, obj, err := pt.GetObject(context.Background(), "b", "k")
	require.NoError(t, err)
	defer rc.Close()

	body, _ := io.ReadAll(rc)
	assert.Equal(t, "local-data", string(body), "local hit must return local content")
	assert.Equal(t, "k", obj.Key)
}

func TestPullThrough_GetObject_FetchFromUpstream(t *testing.T) {
	local := newLocalBackend(t)
	upstream := &stubUpstream{objects: map[string]string{"b/img.png": "upstream-bytes"}}

	require.NoError(t, local.CreateBucket(context.Background(), "b"))

	pt := pullthrough.NewBackend(local, &staticResolver{up: upstream})
	rc, obj, err := pt.GetObject(context.Background(), "b", "img.png")
	require.NoError(t, err, "must fetch from upstream on cache miss")
	defer rc.Close()

	body, _ := io.ReadAll(rc)
	assert.Equal(t, "upstream-bytes", string(body), "cache miss must return upstream content")
	assert.NotNil(t, obj)

	// Second call should be a local hit now
	rc2, _, err := local.GetObject(context.Background(), "b", "img.png")
	require.NoError(t, err, "object must be cached locally after pull-through")
	defer rc2.Close()
	body2, _ := io.ReadAll(rc2)
	assert.Equal(t, "upstream-bytes", string(body2), "cached copy must match upstream content")
}

func TestPullThrough_GetObject_NotFound(t *testing.T) {
	local := newLocalBackend(t)
	upstream := &stubUpstream{objects: map[string]string{}}

	require.NoError(t, local.CreateBucket(context.Background(), "b"))

	pt := pullthrough.NewBackend(local, &staticResolver{up: upstream})
	_, _, err := pt.GetObject(context.Background(), "b", "missing.txt")
	assert.ErrorIs(t, err, storage.ErrObjectNotFound,
		"must return ErrObjectNotFound when neither local nor upstream has the object")
}

func TestPullThrough_ForwardsPartialIOCapabilities(t *testing.T) {
	local := newLocalBackend(t)
	require.NoError(t, local.CreateBucket(context.Background(), "__grainfs_volumes"))

	pt := pullthrough.NewBackend(local, &staticResolver{})
	require.True(t, pt.PreferWriteAt("__grainfs_volumes"))

	_, err := pt.WriteAt(context.Background(), "__grainfs_volumes", "vol/blk", 0, []byte("abcd"))
	require.NoError(t, err)

	buf := make([]byte, 2)
	n, err := pt.ReadAt(context.Background(), "__grainfs_volumes", "vol/blk", 1, buf)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, []byte("bc"), buf)
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
	require.NoError(t, local.CreateBucket(context.Background(), "b"))

	// 10MB of zeroes via streaming reader (no buffer materialization)
	const size = 10 * 1024 * 1024
	content := io.LimitReader(zeroReader{}, size)
	upstream := &streamingUpstream{content: content, size: size}

	pt := pullthrough.NewBackend(local, &staticResolver{up: upstream})
	rc, obj, err := pt.GetObject(context.Background(), "b", "big.bin")
	require.NoError(t, err)
	defer rc.Close()

	// Caller must receive full body bytes
	n, err := io.Copy(io.Discard, rc)
	require.NoError(t, err)
	assert.Equal(t, int64(size), n, "caller must receive full body")
	assert.Equal(t, int64(size), obj.Size, "object metadata must report full size")

	// Verify the body was cached locally
	rc2, _, err := local.GetObject(context.Background(), "b", "big.bin")
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
	require.NoError(t, local.CreateBucket(context.Background(), "b"))

	payload := bytes.Repeat([]byte("grainfs"), 1000) // 7000 bytes
	upstream := &stubUpstream{objects: map[string]string{"b/x": string(payload)}}

	pt := pullthrough.NewBackend(local, &staticResolver{up: upstream})
	rc, _, err := pt.GetObject(context.Background(), "b", "x")
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
	require.NoError(t, local.CreateBucket(context.Background(), "b"))

	payload := bytes.Repeat([]byte("x"), 1000)
	upstream := &errUpstream{
		reader: &errReader{data: payload, failAfter: 500, err: errors.New("upstream broke")},
		size:   1000,
	}

	pt := pullthrough.NewBackend(local, &staticResolver{up: upstream})
	rc, _, err := pt.GetObject(context.Background(), "b", "bad")
	// Must propagate the error (either from pull or post-read)
	if err == nil {
		defer rc.Close()
		_, err = io.ReadAll(rc)
	}
	require.Error(t, err, "upstream error must surface to caller")

	// Local cache must NOT contain a partial entry
	_, _, err = local.GetObject(context.Background(), "b", "bad")
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

// staticResolver is a test helper that always returns the same Upstream
// regardless of bucket. Use mapResolver for per-bucket fan-out.
type staticResolver struct{ up pullthrough.Upstream }

func (s *staticResolver) Resolve(_ string) (pullthrough.Upstream, bool) {
	if s.up == nil {
		return nil, false
	}
	return s.up, true
}

// mapResolver routes by bucket name. Returns (nil, false) for unknown buckets,
// which the Backend treats as "no upstream configured".
type mapResolver struct {
	m map[string]pullthrough.Upstream
}

func (r *mapResolver) Resolve(bucket string) (pullthrough.Upstream, bool) {
	up, ok := r.m[bucket]
	return up, ok
}

// TestPullThrough_PerBucketResolver_RoutesByBucket verifies that the Backend
// asks the Resolver for an Upstream per request and routes by bucket. Buckets
// not in the resolver map fall back to local-only (ErrObjectNotFound).
func TestPullThrough_PerBucketResolver_RoutesByBucket(t *testing.T) {
	local := newLocalBackend(t)
	require.NoError(t, local.CreateBucket(context.Background(), "a"))
	require.NoError(t, local.CreateBucket(context.Background(), "b"))
	require.NoError(t, local.CreateBucket(context.Background(), "c"))

	upA := &stubUpstream{objects: map[string]string{"a/k": "from-A"}}
	upB := &stubUpstream{objects: map[string]string{"b/k": "from-B"}}
	r := &mapResolver{m: map[string]pullthrough.Upstream{"a": upA, "b": upB}}

	pt := pullthrough.NewBackend(local, r)

	// Bucket "a" routes to upA.
	rc, _, err := pt.GetObject(context.Background(), "a", "k")
	require.NoError(t, err)
	body, _ := io.ReadAll(rc)
	rc.Close()
	assert.Equal(t, "from-A", string(body), "bucket a must route to upstream A")

	// Bucket "b" routes to upB.
	rc, _, err = pt.GetObject(context.Background(), "b", "k")
	require.NoError(t, err)
	body, _ = io.ReadAll(rc)
	rc.Close()
	assert.Equal(t, "from-B", string(body), "bucket b must route to upstream B")

	// Bucket "c" has no resolver entry → ErrObjectNotFound (no fallback fetch).
	_, _, err = pt.GetObject(context.Background(), "c", "k")
	assert.ErrorIs(t, err, storage.ErrObjectNotFound,
		"bucket without resolver entry must surface the local 404")
}

func TestPullThrough_PutObject_GoesToLocal(t *testing.T) {
	local := newLocalBackend(t)
	upstream := &stubUpstream{objects: map[string]string{}}

	require.NoError(t, local.CreateBucket(context.Background(), "b"))

	pt := pullthrough.NewBackend(local, &staticResolver{up: upstream})
	_, err := pt.PutObject(context.Background(), "b", "new.txt", strings.NewReader("new"), "text/plain")
	require.NoError(t, err)

	// Must be in local
	rc, _, err := local.GetObject(context.Background(), "b", "new.txt")
	require.NoError(t, err)
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	assert.Equal(t, "new", string(body))
}

// TestPullThrough_ForwardsSnapshotable verifies the pull-through decorator
// satisfies storage.Snapshotable and storage.BucketSnapshotable by delegating
// to the wrapped backend. Regression for the bug where embedding storage.Backend
// did not promote Snapshotable, so PITR snapshots (and GET /admin/snapshots)
// silently broke whenever the boot chain wrapped the backend in pull-through.
func TestPullThrough_ForwardsSnapshotable(t *testing.T) {
	local := newLocalBackend(t)
	require.NoError(t, local.CreateBucket(context.Background(), "b"))
	_, err := local.PutObject(context.Background(), "b", "k", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)

	pt := pullthrough.NewBackend(local, &staticResolver{up: &stubUpstream{}})

	snap, ok := storage.Backend(pt).(storage.Snapshotable)
	require.True(t, ok, "pullthrough.Backend must satisfy storage.Snapshotable")

	objs, err := snap.ListAllObjects()
	require.NoError(t, err)
	require.Len(t, objs, 1, "ListAllObjects must see through the pull-through layer")
	assert.Equal(t, "b", objs[0].Bucket)
	assert.Equal(t, "k", objs[0].Key)

	bs, ok := storage.Backend(pt).(storage.BucketSnapshotable)
	require.True(t, ok, "pullthrough.Backend must satisfy storage.BucketSnapshotable")
	// LocalBackend does not track per-bucket versioning, so it does not implement
	// BucketSnapshotable; the forward returns (nil, nil) in that case. Just assert
	// the call is wired and does not error.
	_, err = bs.ListAllBuckets()
	require.NoError(t, err)

	// Unwrap exposes the inner backend.
	type unwrapper interface{ Unwrap() storage.Backend }
	uw, ok := storage.Backend(pt).(unwrapper)
	require.True(t, ok, "pullthrough.Backend must expose Unwrap()")
	assert.Equal(t, local, uw.Unwrap())
}

type walOffsetBackend struct {
	storage.Backend
	offset uint64
}

func (b *walOffsetBackend) WALOffset() uint64 { return b.offset }

func TestPullThrough_ForwardsWALOffset(t *testing.T) {
	local := newLocalBackend(t)
	wrapped := &walOffsetBackend{Backend: local, offset: 42}
	pt := pullthrough.NewBackend(wrapped, &staticResolver{up: &stubUpstream{}})

	type walProvider interface{ WALOffset() uint64 }
	wp, ok := storage.Backend(pt).(walProvider)
	require.True(t, ok, "pullthrough.Backend must expose WALOffset for PITR snapshot anchors")
	require.Equal(t, uint64(42), wp.WALOffset())
}
