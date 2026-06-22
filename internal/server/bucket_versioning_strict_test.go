package server

import (
	"context"
	"errors"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// erroringVersioner SUPPORTS versioning (implements BucketVersioner) but its
// read fails — as a group-0 leaderless window would when the linearizing
// ReadIndex can't complete. (It must implement SetBucketVersioning too so the
// operations plan selects it as the bucket versioner; otherwise the call
// resolves to UnsupportedOperationError, which the edge tolerates by design.)
type erroringVersioner struct {
	storage.Backend
}

func (erroringVersioner) GetBucketVersioning(string) (string, error) {
	return "", errors.New("no leader: ReadIndex failed")
}

func (erroringVersioner) SetBucketVersioning(string, string) error { return nil }

// TestCtxWithBucketVersioningStrictFailsClosed proves the mutating-edge resolve
// surfaces a versioning-resolve error (fail-closed) instead of defaulting the
// write to non-versioned, while the tolerant read variant still swallows it.
func TestCtxWithBucketVersioningStrictFailsClosed(t *testing.T) {
	real, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	srv := New("127.0.0.1:0", erroringVersioner{Backend: real})

	// STRICT (mutating PUT/Complete/Copy-dst): error must surface.
	_, serr := srv.ctxWithBucketVersioningStrict(context.Background(), "b")
	require.Error(t, serr, "mutating edge must fail-closed on a versioning resolve error")

	// TOLERANT (read): swallows the error, leaves ctx unstamped.
	ctx := srv.ctxWithBucketVersioning(context.Background(), "b")
	require.False(t, ctxVersioningEnabled(ctx), "tolerant variant leaves ctx unstamped on error")
}
