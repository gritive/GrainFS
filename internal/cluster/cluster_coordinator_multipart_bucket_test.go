package cluster

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// TestClusterCoordinatorListMultipartUploads_MissingBucket verifies that the
// coordinator validates bucket existence before fanning out to group backends.
// Group leaves set bypassBucketCheck=true (group_backend.go), so a HeadBucket in
// DistributedBackend alone is a no-op on the fan-out path — without a
// coordinator-level RouteBucket check a missing bucket returns an empty 200
// instead of 404 NoSuchBucket. This is the load-bearing guard for production
// (single-node runs the one-group fan-out path).
func TestClusterCoordinatorListMultipartUploads_MissingBucket(t *testing.T) {
	c := newSingleGroupTestCoordinator(t)

	_, err := c.ListMultipartUploads(context.Background(), "no-such-bucket", "", 0)
	require.ErrorIs(t, err, storage.ErrNoSuchBucket)

	// An existing bucket still lists (empty) without error — no regression.
	uploads, err := c.ListMultipartUploads(context.Background(), "vbucket", "", 0)
	require.NoError(t, err)
	require.Empty(t, uploads)
}
