package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDistributedBackend_isSoleAuthEpochAuthority pins the authority predicate:
// the soleauth epoch is group-0-global, so only the group-0 backend (or a
// legacy/identity single-group backend with empty groupID — test/tooling) is the
// authority allowed to install the shared ShardService's epoch reader. A routed
// data group ("group-N") is NOT the authority.
func TestDistributedBackend_isSoleAuthEpochAuthority(t *testing.T) {
	for _, tc := range []struct {
		id   string
		want bool
	}{
		{"", true},         // legacy/identity single-group (test/tooling)
		{"group-0", true},  // the cluster-global soleauth authority
		{"group-1", false}, // routed data group — never the authority
		{"group-2", false}, // routed data group — never the authority
	} {
		b := &DistributedBackend{groupID: tc.id}
		require.Equal(t, tc.want, b.isSoleAuthEpochAuthority(), "groupID %q", tc.id)
	}
}

// TestSetShardService_DataGroupDoesNotClobberEpochSource is the regression for
// the multi-group clobber. A single ShardService is shared across the node's
// backends; SetShardService wires the soleauth epoch reader as a closure over
// the calling backend's keyspace. group-0 (the authority) wires it correctly at
// boot. A runtime-owned data group then calls SetShardService on the SAME shared
// service — pre-fix it OVERWRITES the reader with a closure over its own keyspace
// (where soleauthepoch:{b} does not exist → reads 0), silently disabling the
// fence (soleAuthEpochStale(0, wire>0) == false → admits a stale wire epoch).
// Post-fix the data group is skipped and the authority's reader survives.
func TestSetShardService_DataGroupDoesNotClobberEpochSource(t *testing.T) {
	ctx := context.Background()

	// Authority backend (groupID "" = legacy/identity authority) holding the
	// global soleauth epoch for bucket "b": off -> pending -> on => epoch 2.
	authority := NewSingletonBackendForTest(t)
	require.NoError(t, authority.CreateBucket(ctx, "b"))
	require.NoError(t, authority.SetBucketSoleAuthority("b", soleAuthPending))
	require.NoError(t, authority.SetBucketSoleAuthority("b", soleAuthOn))
	ep, err := authority.GetBucketSoleAuthEpoch("b")
	require.NoError(t, err)
	require.Equal(t, uint32(2), ep)

	// The authority already wired its epoch reader into its ShardService at
	// construction (NewSingletonBackendForTest) and marked it ready. Sanity: the
	// shared service fences a strictly-older wire epoch against the authority's 2.
	svc := authority.shardSvc
	require.ErrorIs(t, svc.rejectStaleSoleAuthEpoch("b", 1), errStaleSoleAuthEpoch, "wire 1 < local 2 is stale")
	require.NoError(t, svc.rejectStaleSoleAuthEpoch("b", 2), "wire 2 == local 2 is not stale")

	// A runtime-owned data group shares the SAME ShardService and installs its
	// own keyspace reader. Its store has no epoch for "b" (reads 0).
	dataGroup := NewSingletonBackendForTest(t)
	dataGroup.groupID = "group-1" // routed data group — not the authority
	dataGroup.SetShardService(svc, []string{dataGroup.selfAddr})

	// GREEN (post-fix): the data group did NOT clobber the reader, so the shared
	// service still reads the authority's epoch 2 and still fences wire 1.
	// (RED pre-fix: the reader now points at the data group's empty keyspace ->
	// epoch 0 -> rejectStaleSoleAuthEpoch("b", 1) returns nil.)
	require.ErrorIs(t, svc.rejectStaleSoleAuthEpoch("b", 1), errStaleSoleAuthEpoch,
		"a routed data group must NOT clobber the authority's soleauth epoch reader")
	require.NoError(t, svc.rejectStaleSoleAuthEpoch("b", 2))
}

// TestSetShardService_DataGroupStillWiresService asserts the guard is NARROW: a
// non-authority SetShardService still completes the rest of its wiring (it stores
// the service reference and runs the per-FSM SetDEKKeeper/SetFenceLock branch);
// only the epoch-reader install is skipped.
func TestSetShardService_DataGroupStillWiresService(t *testing.T) {
	dataGroup := NewSingletonBackendForTest(t)
	dataGroup.groupID = "group-1"

	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(dataGroup.root, nil, WithShardDEKKeeper(keeper, clusterID))
	dataGroup.SetShardService(svc, []string{dataGroup.selfAddr})

	require.Same(t, svc, dataGroup.shardSvc, "non-authority SetShardService still stores the service reference")
	// The per-FSM fence lock is still wired (shared with the service), proving the
	// guard only gates the epoch reader, not the whole fsm-wiring block.
	require.NotNil(t, dataGroup.fsm.fenceLockFn.Load(), "data group fsm fence lock must still be wired")
}
