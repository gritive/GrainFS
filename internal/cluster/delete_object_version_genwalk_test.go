package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
)

// newForwardGenCoordinator builds a coordinator whose MetaFSM carries the given
// placement generations, with shard groups group-1..group-3 voted by a REMOTE
// peer (so self is never the leader/only-voter and every mutation forwards) and
// a recording dialer to observe which groups each forwarded op targets.
func newForwardGenCoordinator(t *testing.T, generations ...[]string) (*ClusterCoordinator, *recordingDialer) {
	t.Helper()
	base := &fakeBackend{}
	mgr := NewDataGroupManager()
	meta := NewMetaFSM()
	for _, id := range []string{"group-1", "group-2", "group-3"} {
		mgr.Add(NewDataGroup(id, []string{"remote"})) // self not a voter → forward
		require.NoError(t, meta.applyCmd(makePutShardGroupCmd(t, id, []string{"remote"})))
	}
	router := NewRouter(mgr)
	router.AssignBucket("bk", "group-1")
	for _, gen := range generations {
		require.NoError(t, meta.applyCmd(makeAddPlacementGenerationCmd(t, gen)))
	}
	d := &recordingDialer{
		replyByOp:     map[raftpb.ForwardOp][]byte{raftpb.ForwardOpDeleteObjectVersion: buildOKReply()},
		streamReplyBy: map[raftpb.ForwardOp][]byte{},
		readReplyBy:   map[raftpb.ForwardOp][]byte{},
		readBodyBy:    map[raftpb.ForwardOp][]byte{},
	}
	sender := NewForwardSender(d.dial)
	// No WithECConfig: NumShards()==0 so the no-generation fallback resolves via
	// RouteBucket (a single deterministic target — the bucket's assigned group),
	// matching the proven setupCoordWithForward pattern. ECConfig is irrelevant to
	// the multi-generation tests (RouteObjectReadGenerations walks the generation
	// registry directly and never hits the routeWriteOrBucket fallback).
	c := NewClusterCoordinator(base, mgr, router, meta, "self").
		WithForwardSender(sender)
	return c, d
}

// keyWithDistinctGenGroups returns a key whose gen-0 (mod 2) and gen-1 (mod 3)
// placement groups differ, so the fan-out targets two distinct groups.
func keyWithDistinctGenGroups(t *testing.T) string {
	t.Helper()
	for _, k := range []string{
		"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
		"n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
	} {
		g0 := groupIDForObject("bk", k, []string{"group-1", "group-2"})
		g1 := groupIDForObject("bk", k, []string{"group-1", "group-2", "group-3"})
		if g0 != g1 {
			return k
		}
	}
	t.Fatal("expected a key whose generation placement differs")
	return ""
}

// TestDeleteObjectVersion_FansOutAcrossGenerations proves the delete reaches the
// older-generation group that actually holds the version record, not only the
// newest-generation group. This is the Q5 regression: before the fix the delete
// is forwarded to one (newest-gen) group and silently misses the resident record.
func TestDeleteObjectVersion_FansOutAcrossGenerations(t *testing.T) {
	key := keyWithDistinctGenGroups(t)
	c, d := newForwardGenCoordinator(t,
		[]string{"group-1", "group-2"},            // gen-0 (pre-growth)
		[]string{"group-1", "group-2", "group-3"}, // gen-1 (post-growth)
	)

	require.NoError(t, c.DeleteObjectVersion("bk", key, "vid-1"))

	var gids []string
	for _, call := range d.calls {
		require.Equal(t, raftpb.ForwardOpDeleteObjectVersion, call.op)
		gids = append(gids, call.gid)
	}
	wantNewest := groupIDForObject("bk", key, []string{"group-1", "group-2", "group-3"})
	wantBase := groupIDForObject("bk", key, []string{"group-1", "group-2"})
	require.ElementsMatch(t, []string{wantNewest, wantBase}, gids,
		"delete must fan out to every generation group holding a possible record")
}

// TestDeleteObjectVersion_DedupsSharedGroup proves that when a key hashes to the
// SAME group across generations, the delete is issued exactly once (no redundant
// duplicate proposal to the same group).
func TestDeleteObjectVersion_DedupsSharedGroup(t *testing.T) {
	var key string
	for _, k := range []string{
		"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
		"n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
	} {
		g0 := groupIDForObject("bk", k, []string{"group-1", "group-2"})
		g1 := groupIDForObject("bk", k, []string{"group-1", "group-2", "group-3"})
		if g0 == g1 {
			key = k
			break
		}
	}
	require.NotEmpty(t, key, "expected a key whose generation placement coincides")

	c, d := newForwardGenCoordinator(t,
		[]string{"group-1", "group-2"},
		[]string{"group-1", "group-2", "group-3"},
	)

	require.NoError(t, c.DeleteObjectVersion("bk", key, "vid-1"))
	require.Len(t, d.calls, 1, "same group across generations → deduped to one delete")
}

// TestDeleteObjectVersion_SingleGenOneForward proves that with no recorded
// placement generations (the common non-grown deployment) the delete issues
// exactly one forward — byte-identical to the legacy single-target behavior.
func TestDeleteObjectVersion_SingleGenOneForward(t *testing.T) {
	c, d := newForwardGenCoordinator(t) // no generations

	require.NoError(t, c.DeleteObjectVersion("bk", "k", "vid-1"))
	require.Len(t, d.calls, 1, "single generation → exactly one delete (no behavior change)")
	require.Equal(t, raftpb.ForwardOpDeleteObjectVersion, d.calls[0].op)
}

// TestDeleteObjectVersion_FailClosedSurfacesError proves that when a target
// group returns a non-OK status, DeleteObjectVersion returns an error (so the
// client retries the whole idempotent fan-out) yet still attempts every
// (deduped) generation group rather than aborting on the first failure.
func TestDeleteObjectVersion_FailClosedSurfacesError(t *testing.T) {
	key := keyWithDistinctGenGroups(t)
	c, d := newForwardGenCoordinator(t,
		[]string{"group-1", "group-2"},
		[]string{"group-1", "group-2", "group-3"},
	)
	// Override the OK reply: every forwarded delete returns an internal error.
	d.replyByOp[raftpb.ForwardOpDeleteObjectVersion] = buildSimpleReply(raftpb.ForwardStatusInternal, "boom")

	err := c.DeleteObjectVersion("bk", key, "vid-1")
	require.Error(t, err, "a per-target failure must surface so the client retries")

	gids := make([]string, 0, len(d.calls))
	for _, call := range d.calls {
		gids = append(gids, call.gid)
	}
	wantNewest := groupIDForObject("bk", key, []string{"group-1", "group-2", "group-3"})
	wantBase := groupIDForObject("bk", key, []string{"group-1", "group-2"})
	require.ElementsMatch(t, []string{wantNewest, wantBase}, gids,
		"loop must still attempt every generation group despite the first error")
}
