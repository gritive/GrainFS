package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBucketReads_FailFast_WhenMetaBucketStoreUnwired(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	b.SetMetaBucketStore(nil)
	require.ErrorContains(t, b.HeadBucket(ctx, "b"), "MetaBucketStore not wired")
	_, err := b.GetBucketPolicy("b")
	require.ErrorContains(t, err, "MetaBucketStore not wired")
	_, err = b.GetBucketVersioning("b")
	require.ErrorContains(t, err, "MetaBucketStore not wired")
	_, err = b.GetBucketVersioningLinearized(ctx, "b")
	require.ErrorContains(t, err, "MetaBucketStore not wired")
	_, err = b.ListBuckets(ctx)
	require.ErrorContains(t, err, "MetaBucketStore not wired")
}
