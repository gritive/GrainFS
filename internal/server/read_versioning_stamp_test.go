package server

import (
	"context"
	"io"
	"testing"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// versioningStampSpy embeds a real backend and records the bucket-versioning
// decision present in the context handed to each read/list op. Its own
// GetBucketVersioning reports "Suspended" so that — absent a server-edge stamp —
// cluster.bucketVersioningEnabled(ctx) would resolve to a local read and see
// NOT-enabled. The server read/list handlers must stamp the AUTHORITATIVE
// decision (here forced Enabled via the spy's authoritativeEnabled flag, which
// is what GetBucketVersioning returns to the server edge) so the op sees it.
type versioningStampSpy struct {
	storage.Backend
	// what the server edge resolves via GetBucketVersioning (authoritative).
	authoritativeState string
	// recorded ctx flag seen by each op.
	getVersionStamped  bool
	headVersionStamped bool
	getStamped         bool
	headStamped        bool
	listStamped        bool
}

func ctxVersioningEnabled(ctx context.Context) bool {
	enabled, resolved := cluster.BucketVersioningFromContext(ctx)
	return resolved && enabled
}

// GetBucketVersioning is the authoritative read the server edge consults.
// Both methods are defined so the spy satisfies storage.BucketVersioner and the
// Operations plan routes GetBucketVersioning to it (not the embedded backend).
func (s *versioningStampSpy) GetBucketVersioning(bucket string) (string, error) {
	return s.authoritativeState, nil
}

func (s *versioningStampSpy) SetBucketVersioning(bucket, state string) error {
	s.authoritativeState = state
	return nil
}

func (s *versioningStampSpy) GetObjectVersion(ctx context.Context, bucket, key, versionID string) (io.ReadCloser, *storage.Object, error) {
	s.getVersionStamped = ctxVersioningEnabled(ctx)
	return io.NopCloser(nil), &storage.Object{Key: key, VersionID: versionID}, nil
}

func (s *versioningStampSpy) HeadObjectVersion(ctx context.Context, bucket, key, versionID string) (*storage.Object, error) {
	s.headVersionStamped = ctxVersioningEnabled(ctx)
	return &storage.Object{Key: key, VersionID: versionID}, nil
}

func (s *versioningStampSpy) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	s.getStamped = ctxVersioningEnabled(ctx)
	return io.NopCloser(nil), &storage.Object{Key: key}, nil
}

func (s *versioningStampSpy) HeadObject(ctx context.Context, bucket, key string) (*storage.Object, error) {
	s.headStamped = ctxVersioningEnabled(ctx)
	return &storage.Object{Key: key}, nil
}

func (s *versioningStampSpy) ListObjectsPage(ctx context.Context, bucket, prefix, marker string, maxKeys int) ([]*storage.Object, bool, error) {
	s.listStamped = ctxVersioningEnabled(ctx)
	return nil, false, nil
}

// newStampTestServer builds a server whose Operations wrap the spy.
func newStampTestServer(t *testing.T, state string) (*Server, *versioningStampSpy) {
	t.Helper()
	real := cluster.NewSingletonBackendForTest(t)
	spy := &versioningStampSpy{Backend: real, authoritativeState: state}
	srv := New("127.0.0.1:0", spy)
	return srv, spy
}

// TestServerEdgeStampsVersioning_ReadList proves the server read/list handlers
// stamp the authoritative bucket-versioning decision into ctx before calling the
// backend ops — mirroring PUT. RED before Task 2 (handlers don't stamp).
func TestServerEdgeStampsVersioning_ReadList(t *testing.T) {
	srv, spy := newStampTestServer(t, "Enabled")
	ctx := context.Background()
	const bkt, key = "vbkt", "obj"

	_, _, err := srv.loadObjectForGet(ctx, bkt, key, "v1")
	require.NoError(t, err)
	require.True(t, spy.getVersionStamped, "GET ?versionId must carry the Enabled stamp")

	_, err = srv.loadObjectForHead(ctx, bkt, key, "v1")
	require.NoError(t, err)
	require.True(t, spy.headVersionStamped, "HEAD ?versionId must carry the Enabled stamp")

	_, _, err = srv.loadObjectForGet(ctx, bkt, key, "")
	require.NoError(t, err)
	require.True(t, spy.getStamped, "GET (latest) must carry the Enabled stamp")

	_, err = srv.loadObjectForHead(ctx, bkt, key, "")
	require.NoError(t, err)
	require.True(t, spy.headStamped, "HEAD (latest) must carry the Enabled stamp")

	_, _, err = srv.listBucketObjectsPage(ctx, bkt, "", "", 1000)
	require.NoError(t, err)
	require.True(t, spy.listStamped, "LIST must carry the Enabled stamp")
}
