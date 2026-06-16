package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeGenShardSource implements ShardGroupSource + placementGenerationSource so
// gen-0 establishment can be unit-tested: ShardGroups() drives the live
// candidate set, and PlacementGenerations() is settable to simulate "gen-0 not
// yet recorded" (empty) vs "already recorded" (non-empty). The recorder spy
// mutates gens to mirror what the real FSM apply does, so idempotency is exercised.
type fakeGenShardSource struct {
	groups map[string]ShardGroupEntry
	gens   []placementGeneration
}

func (f *fakeGenShardSource) ShardGroup(id string) (ShardGroupEntry, bool) {
	g, ok := f.groups[id]
	return g, ok
}

func (f *fakeGenShardSource) ShardGroups() []ShardGroupEntry {
	out := make([]ShardGroupEntry, 0, len(f.groups))
	for _, g := range f.groups {
		out = append(out, g)
	}
	return out
}

func (f *fakeGenShardSource) PlacementGenerations() []placementGeneration {
	return f.gens
}

// TestEnsureGenZero_RecordsLiveCandidateSet pins the core fix: on the first
// object write, when no generation is recorded yet, the coordinator records
// gen-0 = the live converged candidate set (sorted candidate IDs), and a second
// write is a no-op (gen-0 already in the FSM). This is the consistent gen-0 that
// makes every node route a key to the same group instead of its divergent
// boot-frozen subset.
//
// RED-on-revert: drop ensureGenZero's record call (or the WithGenZeroRecorder
// wiring) and `recorded` stays empty.
func TestEnsureGenZero_RecordsLiveCandidateSet(t *testing.T) {
	ec := ECConfig{DataShards: 2, ParityShards: 1}
	meta := &fakeGenShardSource{groups: map[string]ShardGroupEntry{
		"group-1": {ID: "group-1", PeerIDs: []string{"node-1", "node-2", "node-3"}},
		"group-2": {ID: "group-2", PeerIDs: []string{"node-1", "node-2", "node-3"}},
		"group-3": {ID: "group-3", PeerIDs: []string{"node-1", "node-2", "node-3"}},
	}}
	var recorded [][]string
	c := NewClusterCoordinator(&fakeBackend{}, NewDataGroupManager(), nil, meta, "node-1").
		WithECConfig(ec).
		WithGenZeroRecorder(func(_ context.Context, ids []string) error {
			recorded = append(recorded, append([]string(nil), ids...))
			// Mirror the real FSM apply: gen-0 is now recorded.
			meta.gens = []placementGeneration{{epoch: 0, groupIDs: append([]string(nil), ids...)}}
			return nil
		})

	c.ensureGenZero(context.Background())
	require.Equal(t, [][]string{{"group-1", "group-2", "group-3"}}, recorded,
		"gen-0 = live converged candidate set (sorted)")

	// Idempotent: a later write does not re-record once gen-0 exists.
	c.ensureGenZero(context.Background())
	require.Len(t, recorded, 1, "gen-0 recorded exactly once; later writes are no-ops")
}

// TestEnsureGenZero_BaseIsLiveNotFrozen pins load-bearing detail #1: gen-0 base
// is the LIVE converged candidate set, NOT the OpRouter's boot-frozen subset.
// Groups that join after boot (no rebuild fires on PutShardGroup) must be in
// gen-0 — at formation, with zero objects written, the live set is the correct
// ground truth the first writes will actually use.
//
// RED-on-revert: source gen-0 from currentPlacementGroupIDs() (frozen) and it
// records {group-1, group-2} instead of the full live set.
func TestEnsureGenZero_BaseIsLiveNotFrozen(t *testing.T) {
	ec := ECConfig{DataShards: 2, ParityShards: 1}
	meta := &fakeGenShardSource{groups: map[string]ShardGroupEntry{
		"group-1": {ID: "group-1", PeerIDs: []string{"node-1", "node-2", "node-3"}},
		"group-2": {ID: "group-2", PeerIDs: []string{"node-1", "node-2", "node-3"}},
	}}
	var recorded [][]string
	c := NewClusterCoordinator(&fakeBackend{}, NewDataGroupManager(), nil, meta, "node-1").
		WithECConfig(ec).
		WithGenZeroRecorder(func(_ context.Context, ids []string) error {
			recorded = append(recorded, append([]string(nil), ids...))
			meta.gens = []placementGeneration{{epoch: 0, groupIDs: append([]string(nil), ids...)}}
			return nil
		})

	// Two groups join AFTER boot. No rebuild fires, so the OpRouter base stays
	// frozen at {group-1, group-2}; gen-0 must still capture the live set.
	meta.groups["group-3"] = ShardGroupEntry{ID: "group-3", PeerIDs: []string{"node-1", "node-2", "node-3"}}
	meta.groups["group-4"] = ShardGroupEntry{ID: "group-4", PeerIDs: []string{"node-1", "node-2", "node-3"}}

	c.ensureGenZero(context.Background())
	require.Equal(t, [][]string{{"group-1", "group-2", "group-3", "group-4"}}, recorded,
		"gen-0 base = LIVE converged candidate set, NOT the boot-frozen subset")
}

// TestEnsureGenZero_NoRecorderIsNoOp proves single-node / test wiring (no
// recorder) is unaffected: ensureGenZero returns without touching anything.
func TestEnsureGenZero_NoRecorderIsNoOp(t *testing.T) {
	ec := ECConfig{DataShards: 2, ParityShards: 1}
	meta := &fakeGenShardSource{groups: map[string]ShardGroupEntry{
		"group-1": {ID: "group-1", PeerIDs: []string{"node-1", "node-2", "node-3"}},
	}}
	c := NewClusterCoordinator(&fakeBackend{}, NewDataGroupManager(), nil, meta, "node-1").
		WithECConfig(ec)

	require.NotPanics(t, func() { c.ensureGenZero(context.Background()) })
	require.Empty(t, meta.gens, "no recorder → no generation recorded")
}
