package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// notFoundHeadBackend reports a missing bucket from HeadBucket but a non-not-found
// error from ForceDeleteBucket. It proves Operations.ForceDeleteBucket runs the
// upfront existence check (→ ErrBucketNotFound) BEFORE forwarding to the backend —
// the cluster coordinator's force path resolves versioning state before RouteBucket
// and would otherwise surface an internal error (not 404) for a missing bucket.
type notFoundHeadBackend struct {
	basicBackend
	forceErr error
}

func (b *notFoundHeadBackend) HeadBucket(context.Context, string) error        { return ErrBucketNotFound }
func (b *notFoundHeadBackend) ForceDeleteBucket(context.Context, string) error { return b.forceErr }

func TestOperations_ForceDeleteBucket_MissingBucketIsNotFound(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("internal force error")
	ops := NewOperations(&notFoundHeadBackend{forceErr: sentinel})

	err := ops.ForceDeleteBucket(ctx, "missing")
	require.ErrorIs(t, err, ErrBucketNotFound)
	require.NotErrorIs(t, err, sentinel) // upfront HeadBucket short-circuits before forwarding
}
