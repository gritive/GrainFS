package server

import (
	"context"
	"errors"
	"testing"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// soleAuthEpochSpy embeds a real backend and reports an authoritative committed
// soleauth epoch per bucket via GetBucketSoleAuthEpoch, so the server edge stamp
// can be proven to thread the ORIGINATING node's epoch onto the context.
type soleAuthEpochSpy struct {
	storage.Backend
	epochByBucket map[string]uint32
	err           error
}

func (s *soleAuthEpochSpy) GetBucketSoleAuthEpoch(bucket string) (uint32, error) {
	if s.err != nil {
		return 0, s.err
	}
	return s.epochByBucket[bucket], nil
}

// The spy must satisfy the full BucketVersioner adapter (the Operations plan
// only binds the epoch reader when the backend is a complete BucketVersioner).
func (s *soleAuthEpochSpy) GetBucketVersioning(bucket string) (string, error) {
	return "Suspended", nil
}

func (s *soleAuthEpochSpy) SetBucketVersioning(bucket, state string) error {
	return nil
}

// TestCtxWithSoleAuthEpochStampsResolvedEpoch proves the PUT-edge stamp resolves
// the bucket's committed epoch and threads it onto the context, so a forwarded
// PUT carries the originating node's authoritative epoch.
func TestCtxWithSoleAuthEpochStampsResolvedEpoch(t *testing.T) {
	real, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	spy := &soleAuthEpochSpy{
		Backend:       real,
		epochByBucket: map[string]uint32{"bkt": 7},
	}
	srv := New("127.0.0.1:0", spy)

	ctx := srv.ctxWithSoleAuthEpoch(context.Background(), "bkt")

	epoch, resolved := cluster.BucketSoleAuthEpochFromContext(ctx)
	require.True(t, resolved, "edge must stamp the committed epoch onto the context")
	require.Equal(t, uint32(7), epoch)
}

// TestCtxWithSoleAuthEpochLeavesUnstampedOnError proves a read error leaves the
// context unstamped, so the commit path falls back to a local read.
func TestCtxWithSoleAuthEpochLeavesUnstampedOnError(t *testing.T) {
	real, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	spy := &soleAuthEpochSpy{
		Backend: real,
		err:     errors.New("boom"),
	}
	srv := New("127.0.0.1:0", spy)

	ctx := srv.ctxWithSoleAuthEpoch(context.Background(), "bkt")

	_, resolved := cluster.BucketSoleAuthEpochFromContext(ctx)
	require.False(t, resolved, "a read error must leave the context unstamped")
}
