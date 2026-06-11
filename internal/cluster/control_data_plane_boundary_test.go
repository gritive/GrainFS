package cluster

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestControlDataPlaneBoundary_ObjectHotPathDoesNotTouchControlRaft is the
// Phase 6 S6-1 dynamic regression guard for the control/data plane boundary.
//
// Invariant under test: an object PUT/GET/HEAD critical path routes only through
// the data plane (per-group raft + per-node quorum-meta) and never touches the
// control plane (meta-raft: bucket membership / IAM / multipart manifest).
//
// Topology is deliberately *non-collapsed* so the assertion discriminates:
//   - control plane = `base` (fakeBackend), the stand-in for the coordinator's
//     DistributedBackend whose propose path reaches meta-raft in production. Every
//     method on it records into base.calls, so any control-plane touch is visible.
//   - data plane = `gb`, a *real* group-raft GroupBackend (newTestFollowerGroupBackend
//     builds an actual raft node via newRaftNode), distinct from base.
//
// In a legacy single-backend deployment WrapDistributedBackend makes the group
// backend share base's raft node (gb.node == base.node); a spy there would be
// vacuous. This harness avoids that collapse — base and gb are separate nodes —
// so an empty base.calls after PUT/GET/HEAD is a real boundary signal.
//
// Non-vacuity is proven inline by the positive control at the end: the very same
// spy DOES record a genuine control-plane op (CreateBucket → meta-raft). That
// makes the negative assertion RED-able: were a PUT-path op wired through base,
// base.calls would be non-empty and this test would fail.
func TestControlDataPlaneBoundary_ObjectHotPathDoesNotTouchControlRaft(t *testing.T) {
	base := &fakeBackend{}
	gb := newTestFollowerGroupBackend(t, "g1", "self")
	gb.Node().Start()
	stopApply := make(chan struct{})
	go gb.RunApplyLoop(stopApply)
	t.Cleanup(func() { close(stopApply) })

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("g1", []string{"self"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket("data-bucket", "g1")
	meta := &fakeBucketAssignmentSource{
		fakeShardGroupSource: fakeShardGroupSource{groups: map[string]ShardGroupEntry{
			"g1": {ID: "g1", PeerIDs: []string{"self"}},
		}},
		assignments: map[string]string{"data-bucket": "g1"},
	}
	c := NewClusterCoordinator(base, mgr, router, meta, "self").
		WithECConfig(ECConfig{DataShards: 1, ParityShards: 0})

	ctx := context.Background()

	// PUT → routes to the local group's data-plane backend.
	put, err := c.PutObject(ctx, "data-bucket", "key", strings.NewReader("body"), "text/plain")
	require.NoError(t, err)
	require.Equal(t, int64(4), put.Size)

	// GET → data-plane read (quorum-meta + EC), no control-plane touch.
	rc, getObj, err := c.GetObject(ctx, "data-bucket", "key")
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, int64(4), getObj.Size)

	// HEAD → data-plane metadata read.
	headObj, err := c.HeadObject(ctx, "data-bucket", "key")
	require.NoError(t, err)
	require.Equal(t, int64(4), headObj.Size)

	// BOUNDARY ASSERTION: the object hot path touched the control-plane backend
	// zero times. bucketAssigned() short-circuits on the seeded assignment, so not
	// even the local HeadBucket read fires.
	require.Empty(t, base.calls,
		"object PUT/GET/HEAD must not touch the control-plane (meta-raft) backend; got %v", base.calls)

	// POSITIVE CONTROL: the same spy observes a genuine control-plane op. This
	// proves base.calls is wired to the backend (the negative assertion above is
	// not vacuously empty) and that the boundary check is RED-able by mutation —
	// any PUT-path op rewired through base would surface here too.
	require.NoError(t, c.CreateBucket(ctx, "ctrl-bucket"))
	require.Equal(t, []string{"CreateBucket:ctrl-bucket"}, base.calls,
		"control-plane op must reach the meta-raft backend (proves the spy is non-vacuous)")
}
