package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/stretchr/testify/require"
)

// TestForwardReceiver_ReStampsVersioningState proves the forward receiver
// re-stamps the bucket-versioning tri-state carried in the read/list forward
// args, so a forwarded read on a node that lacks the bucket's versioning state
// still activates the per-version (S2a) path.
//
// Setup: a versioning-enabled bucket with v1, v2; DeleteObjectVersion(v2) purges
// v2's per-version blob (HEAD must re-derive latest = v1) but leaves the stale
// latest-only quorum-meta blob pointing at v2. We then set the bucket to
// "Suspended" so the receiver's LOCAL versioning read resolves NOT-enabled —
// exactly the multi-node situation where the per-version state lives elsewhere.
//
//   - Forward HeadObject with versioning_state=UNKNOWN → receiver falls back to
//     the local (Suspended) read → legacy latest-only path → stale v2 (RED).
//   - Forward HeadObject with versioning_state=ENABLED → receiver re-stamps
//     ENABLED → per-version derive → v1 (GREEN).
func TestForwardReceiver_ReStampsVersioningState(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := t.Context()
	const bkt, key = "vbkt", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	vid1 := putVersioned(t, b, ctx, bkt, key, "content-v1")
	vid2 := putVersioned(t, b, ctx, bkt, key, "content-v2")
	require.NotEqual(t, vid1, vid2)

	// Hard-delete v2's per-version blob: per-version derive now yields v1, but the
	// legacy latest-only blob still resolves to v2.
	require.NoError(t, b.DeleteObjectVersion(bkt, key, vid2))

	// Simulate a receiver node without the bucket's enabled state: a local read
	// would resolve NOT-enabled.
	require.NoError(t, b.SetBucketVersioning(bkt, "Suspended"))

	rcv, mgr := setupReceiver(t, "self")
	gb := WrapDistributedBackend("g1", b)
	mgr.Add(NewDataGroupWithBackend("g1", []string{"self"}, gb))

	headVia := func(state byte) string {
		payload := encodeForwardPayload("g1", raftpb.ForwardOpHeadObject,
			buildHeadObjectArgs(bkt, key, state))
		reply, err := rcv.Handle(payload)
		require.NoError(t, err)
		require.NotNil(t, reply)
		fr := raftpb.GetRootAsForwardReply(reply, 0)
		require.Equal(t, raftpb.ForwardStatusOK, fr.Status(), "HEAD must succeed")
		obj, err := objectFromReply(reply)
		require.NoError(t, err)
		return obj.VersionID
	}

	// UNKNOWN: receiver falls back to local (Suspended) read → stale latest-only v2.
	require.Equal(t, vid2, headVia(versioningStateUnknown),
		"without the stamp the receiver uses the local read → stale v2 (RED pre-fix)")

	// ENABLED: receiver re-stamps → per-version derive → rolled-back latest v1.
	require.Equal(t, vid1, headVia(versioningStateEnabled),
		"the ENABLED stamp must cross the wire and activate the per-version derive → v1")

	// A forwarded GET ?versionId=<deleted v2> must 404 (its per-version blob was
	// purged by DeleteObjectVersion). The ENABLED stamp crosses the wire so the
	// receiver skips the stale latest-only resurrection path.
	gvPayload := encodeForwardPayload("g1", raftpb.ForwardOpGetObjectVersion,
		buildGetObjectVersionArgs(bkt, key, vid2, versioningStateEnabled))
	gvReply, err := rcv.Handle(gvPayload)
	require.NoError(t, err)
	require.NotNil(t, gvReply)
	require.Equal(t, raftpb.ForwardStatusNoSuchKey,
		raftpb.GetRootAsForwardReply(gvReply, 0).Status(),
		"forwarded GET of the hard-deleted version must 404")
}
