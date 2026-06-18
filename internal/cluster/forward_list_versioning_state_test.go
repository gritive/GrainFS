package cluster

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/stretchr/testify/require"
)

// TestForwardListObjects_CarriesAndReStampsVersioningState proves the forward
// ListObjects path carries the bucket-versioning tri-state across the wire and
// the receiver re-stamps it onto the ctx that reaches the backend list — the
// same UNKNOWN-vs-ENABLED differential the HeadObject test
// (TestForwardReceiver_ReStampsVersioningState) proves for the read path.
//
// Unlike HeadObject, ListObjectsPage does not re-derive latest from per-version
// blobs, so the differential is asserted at the wire boundary: the tri-state a
// coordinator stamps into buildListObjectsArgs round-trips through the
// ListObjectsArgs decoder and contextWithVersioningState, yielding the matching
// authoritative ctx decision the receiver hands to dg.Backend().ListObjectsPage.
func TestForwardListObjects_CarriesAndReStampsVersioningState(t *testing.T) {
	stampFromArgs := func(state byte) (enabled, resolved bool) {
		args := buildListObjectsArgs("vbkt", "p/", "", 100, state)
		la := raftpb.GetRootAsListObjectsArgs(args, 0)
		require.Equal(t, state, la.VersioningState(),
			"tri-state must survive the ListObjectsArgs wire round-trip")
		ctx := contextWithVersioningState(context.Background(), la.VersioningState())
		return bucketVersioningFromContext(ctx)
	}

	// UNKNOWN: receiver leaves ctx unresolved → falls back to a local read
	// (mirrors an old peer that omits the field).
	_, resolved := stampFromArgs(versioningStateUnknown)
	require.False(t, resolved,
		"UNKNOWN must leave ctx unresolved so the receiver falls back to a local read")

	// ENABLED: receiver re-stamps ENABLED → the list runs under the
	// authoritative versioning decision without a local versioning read.
	enabled, resolved := stampFromArgs(versioningStateEnabled)
	require.True(t, resolved && enabled,
		"the ENABLED stamp must cross the wire and re-stamp the receiver ctx Enabled")

	// DISABLED: the authoritative not-enabled decision crosses the wire too,
	// so the receiver does not spuriously activate the per-version path.
	enabled, resolved = stampFromArgs(versioningStateDisabled)
	require.True(t, resolved && !enabled,
		"the DISABLED stamp must cross the wire and re-stamp the receiver ctx Disabled")

	// End-to-end through the real receiver: a forwarded ListObjects with the
	// ENABLED stamp dispatches to the backend list and returns the key — the
	// re-stamp at handleListObjects runs in the real Handle path (mirrors
	// TestForwardReceiver_ReStampsVersioningState's headVia).
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := t.Context()
	const bkt, key = "vbkt", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))
	putVersioned(t, b, ctx, bkt, key, "content-v1")
	require.NoError(t, b.SetBucketVersioning(bkt, "Suspended"))

	rcv, mgr := setupReceiver(t, "self")
	gb := WrapDistributedBackend("g1", b)
	mgr.Add(NewDataGroupWithBackend("g1", []string{"self"}, gb))

	listVia := func(state byte) []string {
		payload := encodeForwardPayload("g1", raftpb.ForwardOpListObjects,
			buildListObjectsArgs(bkt, "", "", 100, state))
		reply, err := rcv.Handle(payload)
		require.NoError(t, err)
		require.NotNil(t, reply)
		fr := raftpb.GetRootAsForwardReply(reply, 0)
		require.Equal(t, raftpb.ForwardStatusOK, fr.Status(), "LIST must succeed")
		objs, err := objectsFromReply(reply)
		require.NoError(t, err)
		keys := make([]string, 0, len(objs))
		for _, o := range objs {
			keys = append(keys, o.Key)
		}
		return keys
	}

	require.Equal(t, []string{key}, listVia(versioningStateEnabled),
		"the ENABLED stamp must cross the wire and the receiver re-stamps it before the list")
	require.Equal(t, []string{key}, listVia(versioningStateUnknown),
		"UNKNOWN falls back to a local read and still lists the key")
}

// TestForwardListObjectVersions_CarriesAndReStampsVersioningState proves the
// forward ListObjectVersions path carries the bucket-versioning tri-state
// across the wire and the receiver re-stamps it onto the ctx that reaches
// the backend — mirroring TestForwardListObjects_CarriesAndReStampsVersioningState
// for the versions enumeration path.
func TestForwardListObjectVersions_CarriesAndReStampsVersioningState(t *testing.T) {
	// Wire-round-trip assertions: tri-state survives serialisation and the
	// receiver stamps the matching ctx decision.
	stampFromArgs := func(state byte) (enabled, resolved bool) {
		args := buildListObjectVersionsArgs("vbkt", "p/", 100, state)
		la := raftpb.GetRootAsListObjectVersionsArgs(args, 0)
		require.Equal(t, state, la.VersioningState(),
			"tri-state must survive the ListObjectVersionsArgs wire round-trip")
		ctx := contextWithVersioningState(context.Background(), la.VersioningState())
		return bucketVersioningFromContext(ctx)
	}

	// UNKNOWN → ctx unresolved.
	_, resolved := stampFromArgs(versioningStateUnknown)
	require.False(t, resolved,
		"UNKNOWN must leave ctx unresolved so the receiver falls back to a local read")

	// ENABLED → ctx resolved + enabled.
	enabled, resolved := stampFromArgs(versioningStateEnabled)
	require.True(t, resolved && enabled,
		"ENABLED must cross the wire and re-stamp the receiver ctx Enabled")

	// DISABLED → ctx resolved + not enabled.
	enabled, resolved = stampFromArgs(versioningStateDisabled)
	require.True(t, resolved && !enabled,
		"DISABLED must cross the wire and re-stamp the receiver ctx Disabled")

	// End-to-end through the real receiver: a forwarded ListObjectVersions with
	// the ENABLED stamp must reach the backend with a resolved-enabled ctx.
	// We use the testOnListObjectVersionsCtx hook to capture the ctx the
	// backend's ListObjectVersions sees, then assert it is resolved+enabled.
	gb := newTestGroupBackend(t, "g1")
	ctx := t.Context()
	const bkt = "vbkt3"
	require.NoError(t, gb.CreateBucket(ctx, bkt))

	var capturedCtx context.Context
	gb.DistributedBackend.testOnListObjectVersionsCtx = func(c context.Context) {
		capturedCtx = c
	}

	rcv, mgr := setupReceiver(t, "self")
	mgr.Add(NewDataGroupWithBackend("g1", []string{"self"}, gb))

	payload := encodeForwardPayload("g1", raftpb.ForwardOpListObjectVersions,
		buildListObjectVersionsArgs(bkt, "", 100, versioningStateEnabled))
	reply, err := rcv.Handle(payload)
	require.NoError(t, err)
	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	require.Equal(t, raftpb.ForwardStatusOK, fr.Status(), "LIST versions must succeed")

	require.NotNil(t, capturedCtx, "testOnListObjectVersionsCtx hook must have fired")
	gotEnabled, gotResolved := bucketVersioningFromContext(capturedCtx)
	require.True(t, gotResolved && gotEnabled,
		"receiver must re-stamp ENABLED into the ctx it hands the backend: resolved=%v enabled=%v",
		gotResolved, gotEnabled)
}
