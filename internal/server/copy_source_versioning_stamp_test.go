package server

import (
	"context"
	"io"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// copyVersioningSpy embeds a real backend and records, per bucket, the
// bucket-versioning decision present in the context handed to the source
// per-version read (GetObjectVersion). It reports an authoritative versioning
// state PER BUCKET via GetBucketVersioning so a CopyObject from a
// versioning-enabled SOURCE into a non-versioned DESTINATION can prove the
// source read is gated by the SOURCE bucket's decision, not the destination's.
type copyVersioningSpy struct {
	storage.Backend
	// authoritative state the server edge resolves, keyed by bucket.
	stateByBucket map[string]string
	// recorded: was the ctx that reached the source GetObjectVersion stamped
	// Enabled?
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
// DESTINATION bucket is non-versioned. Pre-fix the copy stamps ctx with the
// destination's (Disabled) decision and reuses it for the source read, so the
// source read resolves the wrong gate (RED).
func TestCopySourceReadUsesSourceBucketVersioning(t *testing.T) {
	real, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, real.CreateBucket(context.Background(), "src-bkt"))
	require.NoError(t, real.CreateBucket(context.Background(), "dst-bkt"))
	spy := &copyVersioningSpy{
		Backend: real,
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

	_, err = srv.copyObjectWithMutation(ctx, req)
	require.NoError(t, err)

	require.True(t, spy.srcGetVersionSeen, "source per-version read must run")
	require.True(t, spy.srcGetVersionStamped,
		"source per-version read must carry the SOURCE bucket's Enabled stamp, not the destination's Disabled decision")
}
