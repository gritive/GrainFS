package server

import (
	"context"
	"io"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// copyDstStub is a minimal in-memory copy destination. copyVersioningSpy embeds
// it so the source-versioning-stamp assertion — the actual subject — can run
// without a real backend's object-write path (the singleton DistributedBackend
// rejects the zero-length streaming copy write for lack of a shard group; the
// destination write is incidental to this test).
type copyDstStub struct {
	storage.Backend
}

func (copyDstStub) CreateBucket(context.Context, string) error { return nil }

func (copyDstStub) ListBuckets(context.Context) ([]string, error) { return nil, nil }

func (copyDstStub) HeadObject(context.Context, string, string) (*storage.Object, error) {
	return nil, storage.ErrObjectNotFound
}

func (copyDstStub) GetObject(context.Context, string, string) (io.ReadCloser, *storage.Object, error) {
	return nil, nil, storage.ErrObjectNotFound
}

func (copyDstStub) PutObjectWithRequest(_ context.Context, req storage.PutObjectRequest) (*storage.Object, error) {
	if req.Body != nil {
		_, _ = io.Copy(io.Discard, req.Body)
	}
	return &storage.Object{Key: req.Key, ContentType: req.ContentType, ETag: "stub-etag", Size: 0}, nil
}

func (copyDstStub) PutObject(_ context.Context, _, key string, r io.Reader, contentType string) (*storage.Object, error) {
	if r != nil {
		_, _ = io.Copy(io.Discard, r)
	}
	return &storage.Object{Key: key, ContentType: contentType, ETag: "stub-etag", Size: 0}, nil
}

// copyVersioningSpy embeds a backend and records, per bucket, the
// bucket-versioning decision present in the context handed to the source
// per-version read (GetObjectVersion / HeadObjectVersion). It reports an
// authoritative versioning state PER BUCKET via GetBucketVersioning so a
// CopyObject from a versioning-enabled SOURCE into a non-versioned DESTINATION
// can prove the source read is gated by the SOURCE bucket's decision, not the
// destination's.
type copyVersioningSpy struct {
	storage.Backend
	stateByBucket        map[string]string
	srcGetVersionStamped bool
	srcGetVersionSeen    bool
}

func (s *copyVersioningSpy) GetBucketVersioning(bucket string) (string, error) {
	if st, ok := s.stateByBucket[bucket]; ok {
		return st, nil
	}
	return "Suspended", nil
}

func (s *copyVersioningSpy) SetBucketVersioning(bucket, state string) error {
	s.stateByBucket[bucket] = state
	return nil
}

func (s *copyVersioningSpy) GetObjectVersion(ctx context.Context, bucket, key, versionID string) (io.ReadCloser, *storage.Object, error) {
	s.srcGetVersionSeen = true
	s.srcGetVersionStamped = ctxVersioningEnabled(ctx)
	return io.NopCloser(io.LimitReader(nil, 0)), &storage.Object{Key: key, VersionID: versionID}, nil
}

func (s *copyVersioningSpy) HeadObjectVersion(ctx context.Context, bucket, key, versionID string) (*storage.Object, error) {
	s.srcGetVersionSeen = true
	s.srcGetVersionStamped = ctxVersioningEnabled(ctx)
	return &storage.Object{Key: key, VersionID: versionID}, nil
}

// TestCopySourceReadUsesSourceBucketVersioning proves the SOURCE per-version
// read is gated by the SOURCE bucket's versioning decision even when the
// DESTINATION bucket is non-versioned. A regression that stamps the
// destination's (Suspended) decision onto the source read would resolve the
// wrong gate for a cross-bucket copy and read the wrong object version.
func TestCopySourceReadUsesSourceBucketVersioning(t *testing.T) {
	spy := &copyVersioningSpy{
		Backend: copyDstStub{},
		stateByBucket: map[string]string{
			"src-bkt": "Enabled",   // source: versioning-enabled
			"dst-bkt": "Suspended", // destination: non-versioned
		},
	}
	srv := New("127.0.0.1:0", spy)

	// Stamp the DESTINATION decision onto ctx exactly as the handler does
	// (copy_object_api.go) before invoking the copy mutation.
	ctx := srv.ctxWithBucketVersioning(context.Background(), "dst-bkt")

	req := storage.CopyObjectRequest{
		Source:            storage.ObjectRef{Bucket: "src-bkt", Key: "obj", VersionID: "v1"},
		Destination:       storage.ObjectRef{Bucket: "dst-bkt", Key: "obj"},
		MetadataDirective: storage.CopyMetadataReplace, // forces the read+rewrite copy path
		ContentType:       "text/plain",
	}

	_, err := srv.copyObjectWithMutation(ctx, req)
	require.NoError(t, err)

	require.True(t, spy.srcGetVersionSeen, "source per-version read must run")
	require.True(t, spy.srcGetVersionStamped,
		"source per-version read must carry the SOURCE bucket's Enabled stamp, not the destination's Disabled decision")
}
