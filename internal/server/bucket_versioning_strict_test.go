package server

import (
	"context"
	"errors"
	"testing"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// erroringVersioner SUPPORTS versioning (implements BucketVersioner) but its read
// returns a genuine fault (e.g. a local store error). It must implement
// SetBucketVersioning too so the operations plan selects it as the bucket
// versioner; otherwise the call resolves to UnsupportedOperationError, which the
// edge tolerates by design.
type erroringVersioner struct {
	storage.Backend
}

func (erroringVersioner) GetBucketVersioning(string) (string, error) {
	return "", errors.New("bucket versioning read fault")
}

func (erroringVersioner) SetBucketVersioning(string, string) error { return nil }

// TestCtxWithBucketVersioningStrictFailsClosedOnFault proves the mutating-edge
// resolve surfaces a GENUINE versioning-read fault (fail-closed) instead of
// defaulting the write to non-versioned, while the tolerant read variant swallows
// it. (A group-0 leaderless window is handled separately by
// GetBucketVersioningLinearized degrading to a local read — it does NOT surface
// as an error here.)
func TestCtxWithBucketVersioningStrictFailsClosedOnFault(t *testing.T) {
	real := cluster.NewSingletonBackendForTest(t)
	srv := New("127.0.0.1:0", erroringVersioner{Backend: real})

	_, serr := srv.ctxWithBucketVersioningStrict(context.Background(), "b")
	require.Error(t, serr, "mutating edge must fail-closed on a genuine versioning-read fault")

	ctx := srv.ctxWithBucketVersioning(context.Background(), "b")
	require.False(t, ctxVersioningEnabled(ctx), "tolerant variant leaves ctx unstamped on a read fault")
}

// recordingVersioner records which resolver the server edge selects.
type recordingVersioner struct {
	storage.Backend
	plainCalls int
	linCalls   int
}

func (r *recordingVersioner) GetBucketVersioning(string) (string, error) {
	r.plainCalls++
	return "Enabled", nil
}
func (r *recordingVersioner) SetBucketVersioning(string, string) error { return nil }
func (r *recordingVersioner) GetBucketVersioningLinearized(context.Context, string) (string, error) {
	r.linCalls++
	return "Enabled", nil
}

// TestEdgeResolverSelection guards the read/write split: READ paths must use the
// PLAIN read (never the linearizing barrier, so reads stay available during a
// group-0 outage), and the MUTATING edge must use the linearized read.
func TestEdgeResolverSelection(t *testing.T) {
	real := cluster.NewSingletonBackendForTest(t)
	rv := &recordingVersioner{Backend: real}
	srv := New("127.0.0.1:0", rv)

	srv.ctxWithBucketVersioning(context.Background(), "b")
	require.Equal(t, 1, rv.plainCalls)
	require.Equal(t, 0, rv.linCalls, "read path must NOT use the linearizing barrier")

	_, serr := srv.ctxWithBucketVersioningStrict(context.Background(), "b")
	require.NoError(t, serr)
	require.Equal(t, 1, rv.linCalls, "mutating edge must use the linearizing read")
	require.Equal(t, 1, rv.plainCalls, "mutating edge must not also do a plain read")
}
