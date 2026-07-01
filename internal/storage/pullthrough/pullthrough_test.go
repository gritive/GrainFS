package pullthrough_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
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
	return cluster.NewSingletonBackendForTest(t)
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

// partialIOInner is a minimal in-memory storage.Backend that ALSO implements
// storage.PartialIO (WriteAt/ReadAt/Truncate) + PreferWriteAt. It exists to
// test that pullthrough forwards the PartialIO capability of its inner — the
// production single-node inner (ClusterCoordinator) provides PartialIO, so a
// PartialIO-capable inner is the faithful substrate for this forwarding test.
// The embedded interface panics on any method the test does not exercise.
type partialIOInner struct {
	storage.Backend
	data map[string][]byte
}

func newPartialIOInner() *partialIOInner { return &partialIOInner{data: map[string][]byte{}} }

func (p *partialIOInner) CreateBucket(context.Context, string) error { return nil }
func (p *partialIOInner) PreferWriteAt(string) bool                  { return true }

func (p *partialIOInner) WriteAt(_ context.Context, bucket, key string, offset uint64, data []byte) (*storage.Object, error) {
	k := bucket + "/" + key
	buf := p.data[k]
	if end := int(offset) + len(data); end > len(buf) {
		nb := make([]byte, end)
		copy(nb, buf)
		buf = nb
	}
	copy(buf[offset:], data)
	p.data[k] = buf
	return &storage.Object{Key: key, Size: int64(len(buf))}, nil
}

func (p *partialIOInner) ReadAt(_ context.Context, bucket, key string, offset int64, buf []byte) (int, error) {
	d := p.data[bucket+"/"+key]
	if offset >= int64(len(d)) {
		return 0, io.EOF
	}
	return copy(buf, d[offset:]), nil
}

func (p *partialIOInner) Truncate(_ context.Context, bucket, key string, size int64) error {
	k := bucket + "/" + key
	if d := p.data[k]; int64(len(d)) > size {
		p.data[k] = d[:size]
	}
	return nil
}

func TestPullThrough_ForwardsPartialIOCapabilities(t *testing.T) {
	local := newPartialIOInner()
	require.NoError(t, local.CreateBucket(context.Background(), "__grainfs_test_internal"))

	pt := pullthrough.NewBackend(local, &staticResolver{})
	require.True(t, pt.PreferWriteAt("__grainfs_test_internal"))

	_, err := pt.WriteAt(context.Background(), "__grainfs_test_internal", "vol/blk", 0, []byte("abcd"))
	require.NoError(t, err)

	buf := make([]byte, 2)
	n, err := pt.ReadAt(context.Background(), "__grainfs_test_internal", "vol/blk", 1, buf)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, []byte("bc"), buf)
}

type recordingPreparedReadAtBackend struct {
	storage.Backend
	calls int
	obj   *storage.Object
}

func (b *recordingPreparedReadAtBackend) ReadAtObject(ctx context.Context, bucket, key string, obj *storage.Object, offset int64, buf []byte) (int, error) {
	b.calls++
	b.obj = obj
	return copy(buf, "prepared"), nil
}

func TestPullThrough_ReadAtObject_DelegatesToInner(t *testing.T) {
	local := newLocalBackend(t)
	rec := &recordingPreparedReadAtBackend{Backend: local}
	pt := pullthrough.NewBackend(rec, &staticResolver{})

	reader, ok := any(pt).(interface {
		ReadAtObject(context.Context, string, string, *storage.Object, int64, []byte) (int, error)
	})
	require.True(t, ok, "pullthrough.Backend must expose prepared ReadAtObject so wrappers do not force a second metadata lookup")

	obj := &storage.Object{Key: "k", Size: 8, ETag: "etag"}
	buf := make([]byte, 8)
	n, err := reader.ReadAtObject(context.Background(), "b", "k", obj, 0, buf)
	require.NoError(t, err)
	require.Equal(t, 8, n)
	require.Equal(t, []byte("prepared"), buf)
	require.Equal(t, 1, rec.calls)
	require.Same(t, obj, rec.obj)
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

// mismatchUpstream reports a Content-Length (Size) that differs from the number
// of bytes its reader actually yields — a truncating or over-long upstream.
type mismatchUpstream struct {
	body         string
	reportedSize int64
}

func (u *mismatchUpstream) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	return io.NopCloser(strings.NewReader(u.body)), &storage.Object{
		Key:  key,
		Size: u.reportedSize,
		ETag: "mismatch",
	}, nil
}

// exactSizeEnforcingBackend stands in for the production cluster DistributedBackend:
// when SizeHintExact is set it enforces the exact body length (the real
// exactObjectSizeReader contract, which is unexported in the cluster package) and
// commits NOTHING on a mismatch — the body never reaches the embedded store. This
// is the collaborator pullthrough's SizeHintExact threading is designed to engage.
type exactSizeEnforcingBackend struct {
	storage.Backend
}

func (b *exactSizeEnforcingBackend) PutObjectWithRequest(ctx context.Context, req storage.PutObjectRequest) (*storage.Object, error) {
	if req.SizeHint != nil && req.SizeHintExact {
		buf, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		if int64(len(buf)) != *req.SizeHint {
			// Reject BEFORE delegating, so nothing is persisted/made visible —
			// exactly as the cluster path's exactObjectSizeReader does.
			return nil, fmt.Errorf("size mismatch: got %d, want %d", len(buf), *req.SizeHint)
		}
		req.Body = bytes.NewReader(buf)
	}
	return b.Backend.(storage.RequestPutter).PutObjectWithRequest(ctx, req)
}

// TestPullThrough_CacheFill_SizeMismatch_FailsAndCachesNothing proves that when an
// upstream's actual body length disagrees with its reported Content-Length, the
// cache-fill (which threads SizeHintExact) fails against a size-enforcing inner
// backend, the GET surfaces the error, and NO partial object is cached or served.
// This is the safe consequence of threading the exact size — the old behavior
// silently cached a truncated object. Covers both truncating and over-long.
func TestPullThrough_CacheFill_SizeMismatch_FailsAndCachesNothing(t *testing.T) {
	cases := []struct {
		name         string
		body         string
		reportedSize int64
	}{
		{"truncating (fewer bytes than reported)", "short", 100},
		{"over-long (more bytes than reported)", strings.Repeat("x", 100), 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			local := newLocalBackend(t)
			require.NoError(t, local.CreateBucket(context.Background(), "b"))
			inner := &exactSizeEnforcingBackend{Backend: local}

			upstream := &mismatchUpstream{body: tc.body, reportedSize: tc.reportedSize}
			pt := pullthrough.NewBackend(inner, &staticResolver{up: upstream})

			rc, _, err := pt.GetObject(context.Background(), "b", "k")
			if err == nil {
				defer rc.Close()
				_, err = io.ReadAll(rc)
			}
			require.Error(t, err, "a size-mismatched upstream must fail the GET, not serve a truncated object")

			// No partial object must remain in the cache: a follow-up GET still misses.
			_, _, err = local.GetObject(context.Background(), "b", "k")
			assert.ErrorIs(t, err, storage.ErrObjectNotFound,
				"a failed size-mismatched cache fill must leave no partial object")
		})
	}
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

// recordingRequestPutterBackend captures the PutObjectRequest the pullthrough
// cache-fill threads into the inner backend, while delegating storage to a real
// LocalBackend so the 2-pass GetObject still works.
type recordingRequestPutterBackend struct {
	storage.Backend
	lastReq *storage.PutObjectRequest
}

func (b *recordingRequestPutterBackend) PutObjectWithRequest(ctx context.Context, req storage.PutObjectRequest) (*storage.Object, error) {
	cp := req
	b.lastReq = &cp
	return b.Backend.(storage.RequestPutter).PutObjectWithRequest(ctx, req)
}

// TestPullThrough_CacheFill_ThreadsExactSizeHint proves the cache-fill write
// threads the upstream's exact size as SizeHintExact so the inner backend takes
// the no-spool streaming path instead of staging the body to a temp file. The
// recording backend captures the request the pullthrough decorator builds.
func TestPullThrough_CacheFill_ThreadsExactSizeHint(t *testing.T) {
	local := newLocalBackend(t)
	require.NoError(t, local.CreateBucket(context.Background(), "b"))
	rec := &recordingRequestPutterBackend{Backend: local}

	const content = "upstream cache fill body"
	upstream := &stubUpstream{objects: map[string]string{"b/k": content}}
	pt := pullthrough.NewBackend(rec, &staticResolver{up: upstream})

	rc, _, err := pt.GetObject(context.Background(), "b", "k")
	require.NoError(t, err)
	body, _ := io.ReadAll(rc)
	require.NoError(t, rc.Close())
	assert.Equal(t, content, string(body), "cache-miss must return upstream content")

	require.NotNil(t, rec.lastReq, "cache fill must go through PutObjectWithRequest (sized path)")
	require.NotNil(t, rec.lastReq.SizeHint, "cache fill must thread a SizeHint")
	assert.Equal(t, int64(len(content)), *rec.lastReq.SizeHint, "SizeHint must equal upstream Size")
	assert.True(t, rec.lastReq.SizeHintExact, "SizeHint must be exact so the inner backend streams (no spool)")
}
