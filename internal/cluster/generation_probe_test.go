package cluster

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestRouteObjectReadGenerations_SingleGenByteIdentical proves that, with no
// topology generations recorded (the default), the generation-aware read router
// returns exactly one target equal to the legacy single-target RouteObjectRead —
// the byte-identical invariant.
func TestRouteObjectReadGenerations_SingleGenByteIdentical(t *testing.T) {
	ec := ECConfig{DataShards: 2, ParityShards: 1}
	groups := newFakeShardGroupSourceN(t, 5)
	r := NewOpRouter(nil, groups, nil, nil, ec, "node-1", nil)

	for _, key := range []string{"a", "obj/1", "k-zzz", "", "x"} {
		primary, _, perr := r.RouteObjectRead("b", key, "")
		require.NoError(t, perr)
		gens, gerr := r.RouteObjectReadGenerations("b", key, "")
		require.NoError(t, gerr)
		require.Len(t, gens, 1, "single generation → one target")
		require.Equal(t, primary.GroupID, gens[0].GroupID, "byte-identical to RouteObjectRead")
	}
}

// TestRouteObjectReadGenerations_MultiGenNewestFirst proves that with two
// recorded generations the router returns one target per generation, newest
// first (latest generation's placement, then the base generation's).
func TestRouteObjectReadGenerations_MultiGenNewestFirst(t *testing.T) {
	ec := ECConfig{DataShards: 2, ParityShards: 1}
	groups := newFakeShardGroupSourceN(t, 3) // group-1..group-3
	r := NewOpRouter(nil, groups, nil, nil, ec, "node-1", nil)
	r.applyGenerations([]placementGeneration{
		{epoch: 0, groupIDs: []string{"group-1", "group-2"}},
		{epoch: 1, groupIDs: []string{"group-1", "group-2", "group-3"}},
	})

	// Find a key whose gen-0 (mod 2) and gen-1 (mod 3) groups differ so the
	// two targets are distinct and order is observable.
	var key string
	for _, k := range []string{"a", "b", "c", "d", "e", "f", "g"} {
		g0 := groupIDForObject("bk", k, []string{"group-1", "group-2"})
		g1 := groupIDForObject("bk", k, []string{"group-1", "group-2", "group-3"})
		if g0 != g1 {
			key = k
			break
		}
	}
	require.NotEmpty(t, key, "expected a key whose generation placement differs")

	gens, err := r.RouteObjectReadGenerations("bk", key, "")
	require.NoError(t, err)
	require.Len(t, gens, 2, "two generations → two targets")
	wantNewest := groupIDForObject("bk", key, []string{"group-1", "group-2", "group-3"})
	wantBase := groupIDForObject("bk", key, []string{"group-1", "group-2"})
	require.Equal(t, wantNewest, gens[0].GroupID, "newest generation first")
	require.Equal(t, wantBase, gens[1].GroupID, "base generation last")
}

// newProbeCoordinator builds a coordinator whose FSM optionally carries
// placement generations, with shard groups group-1..group-3 voted by self so
// routeGroup resolves.
func newProbeCoordinator(t *testing.T, generations ...[]string) *ClusterCoordinator {
	t.Helper()
	mgr := NewDataGroupManager()
	meta := NewMetaFSM()
	for _, id := range []string{"group-1", "group-2", "group-3"} {
		mgr.Add(NewDataGroup(id, []string{"self"}))
		require.NoError(t, meta.applyCmd(makePutShardGroupCmd(t, id, []string{"self"})))
	}
	router := NewRouter(mgr)
	router.AssignBucket("bk", "group-1")
	for _, gen := range generations {
		require.NoError(t, meta.applyCmd(makeAddPlacementGenerationCmd(t, gen)))
	}
	return NewClusterCoordinator(&fakeBackend{}, mgr, router, meta, "self").
		WithECConfig(ECConfig{DataShards: 2, ParityShards: 1})
}

// TestProbeRead_AdvancesOnNotFound proves probeRead falls through to the older
// generation when the newest returns a definitive not-found, and stops at the
// first hit.
func TestProbeRead_AdvancesOnNotFound(t *testing.T) {
	c := newProbeCoordinator(t,
		[]string{"group-1", "group-2"},            // gen-0
		[]string{"group-1", "group-2", "group-3"}, // gen-1
	)
	var seen []string
	err := c.probeRead("bk", "k-zzz", "", func(target RouteTarget) error {
		seen = append(seen, target.GroupID)
		if len(seen) == 1 {
			return storage.ErrObjectNotFound // newest gen misses → probe older
		}
		return nil // found in older gen
	})
	require.NoError(t, err)
	require.Len(t, seen, 2, "probed newest then base generation")
}

// TestProbeRead_FailClosed proves a non-not-found error (group unavailable)
// aborts the probe immediately rather than masquerading as a 404 by falling
// through to an older generation.
func TestProbeRead_FailClosed(t *testing.T) {
	c := newProbeCoordinator(t,
		[]string{"group-1", "group-2"},
		[]string{"group-1", "group-2", "group-3"},
	)
	boom := errors.New("group unavailable")
	var calls int
	err := c.probeRead("bk", "k-zzz", "", func(target RouteTarget) error {
		calls++
		return boom
	})
	require.ErrorIs(t, err, boom, "non-not-found error returned immediately (fail-closed)")
	require.Equal(t, 1, calls, "must not probe the next generation after a real error")
}

// TestProbeRead_SingleGenOneAttempt proves the default (no generations) path
// makes exactly one attempt — byte-identical to legacy single-target routing.
func TestProbeRead_SingleGenOneAttempt(t *testing.T) {
	c := newProbeCoordinator(t) // no generations
	var calls int
	err := c.probeRead("bk", "k-zzz", "", func(target RouteTarget) error {
		calls++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, calls, "single generation → exactly one attempt")
}
