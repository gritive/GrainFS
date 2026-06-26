package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

type fakeRosterUpdater struct {
	mu       sync.Mutex
	proposed []ShardGroupEntry
	errs     map[string]error
}

func (u *fakeRosterUpdater) ProposeShardGroupForwarding(_ context.Context, sg ShardGroupEntry) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.proposed = append(u.proposed, sg)
	if u.errs != nil {
		if err, ok := u.errs[sg.ID]; ok {
			return err
		}
	}
	return nil
}

func revokedSet(ids ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		m[id] = struct{}{}
	}
	return m
}

func TestEvacuator_MovesRevokedPeerToHealthyPeer(t *testing.T) {
	updater := &fakeRosterUpdater{}
	ev := newDataGroupEvacuatorForTest(revokedSet("node-b"), []ShardGroupEntry{
		{ID: "group-7", PeerIDs: []string{"node-a", "node-b", "node-c"}},
	}, updater, func(string, map[string]struct{}) (string, bool) { return "node-d", true })

	ev.reconcileOnce(context.Background())

	require.Equal(t, []ShardGroupEntry{
		{ID: "group-7", PeerIDs: []string{"node-a", "node-c", "node-d"}},
	}, updater.proposed)
}

func TestEvacuator_ShrinksWhenNoHealthyCandidate(t *testing.T) {
	updater := &fakeRosterUpdater{}
	ev := newDataGroupEvacuatorForTest(revokedSet("node-b"), []ShardGroupEntry{
		{ID: "group-7", PeerIDs: []string{"node-a", "node-b"}},
	}, updater, func(string, map[string]struct{}) (string, bool) { return "", false })

	ev.reconcileOnce(context.Background())

	require.Equal(t, []ShardGroupEntry{
		{ID: "group-7", PeerIDs: []string{"node-a"}},
	}, updater.proposed)
}

func TestEvacuator_NoTargets_NoOp(t *testing.T) {
	updater := &fakeRosterUpdater{}
	ev := newDataGroupEvacuatorForTest(revokedSet("node-b"), []ShardGroupEntry{
		{ID: "group-7", PeerIDs: []string{"node-a", "node-c"}},
	}, updater, func(string, map[string]struct{}) (string, bool) { return "", false })

	ev.reconcileOnce(context.Background())

	require.Empty(t, updater.proposed)
}

func TestEvacuator_ProposalErrorIsNotFatal(t *testing.T) {
	updater := &fakeRosterUpdater{errs: map[string]error{"group-7": context.DeadlineExceeded}}
	ev := newDataGroupEvacuatorForTest(revokedSet("node-b"), []ShardGroupEntry{
		{ID: "group-7", PeerIDs: []string{"node-a", "node-b", "node-c"}},
	}, updater, func(string, map[string]struct{}) (string, bool) { return "node-d", true })

	ev.reconcileOnce(context.Background())

	require.Len(t, updater.proposed, 1)
}

func TestPickHealthyExcluding_SkipsExcludedAndPicksLightest(t *testing.T) {
	loads := map[string]float64{"a": 70, "b": 10, "c": 30, "d": 5}
	loadFn := func(id string) (float64, bool) {
		l, ok := loads[id]
		return l, ok
	}
	candidates := []string{"a", "b", "c", "d"}

	got, ok := PickHealthyExcluding(candidates, loadFn, revokedSet("d"))
	require.True(t, ok)
	require.Equal(t, "b", got)

	_, ok = PickHealthyExcluding(candidates, loadFn, revokedSet("a", "b", "c", "d"))
	require.False(t, ok)

	got, ok = PickHealthyExcluding(append(candidates, "e"), loadFn, revokedSet("d"))
	require.True(t, ok)
	require.Equal(t, "b", got)

	_, ok = PickHealthyExcluding([]string{"e"}, loadFn, map[string]struct{}{})
	require.False(t, ok)
}

func TestEvacuator_DoesNotMoveToAnotherRevokedNode(t *testing.T) {
	var capturedExclude map[string]struct{}
	updater := &fakeRosterUpdater{}
	ev := newDataGroupEvacuatorForTest(revokedSet("node-b", "node-d"), []ShardGroupEntry{
		{ID: "group-7", PeerIDs: []string{"node-a", "node-b", "node-c"}},
	}, updater, func(_ string, exclude map[string]struct{}) (string, bool) {
		capturedExclude = exclude
		return "", false
	})

	ev.reconcileOnce(context.Background())

	_, hasB := capturedExclude["node-b"]
	_, hasD := capturedExclude["node-d"]
	require.True(t, hasB, "revoked target must be excluded")
	require.True(t, hasD, "other revoked nodes must be excluded as candidates")
}

func newDataGroupEvacuatorForTest(
	revoked map[string]struct{},
	groups []ShardGroupEntry,
	updater shardGroupRosterUpdater,
	pickHealthy func(string, map[string]struct{}) (string, bool),
) *DataGroupEvacuator {
	return &DataGroupEvacuator{
		src:         staticRosterSource{revoked: revoked, groups: groups},
		updater:     updater,
		logger:      log.With().Str("component", "evacuator-test").Logger(),
		pickHealthy: pickHealthy,
		tick:        time.Millisecond,
		wakeCh:      make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
		ledTargets: func() []evacTarget {
			var out []evacTarget
			for _, sg := range groups {
				for _, p := range sg.PeerIDs {
					if _, ok := revoked[p]; ok {
						out = append(out, evacTarget{groupID: sg.ID, revokedNode: p, peerIDs: sg.PeerIDs})
					}
				}
			}
			return out
		},
	}
}

type staticRosterSource struct {
	revoked map[string]struct{}
	groups  []ShardGroupEntry
}

func (s staticRosterSource) RevokedNodeIDs() map[string]struct{} { return s.revoked }

func (s staticRosterSource) ShardGroups() []ShardGroupEntry {
	return append([]ShardGroupEntry(nil), s.groups...)
}

func (s staticRosterSource) ShardGroup(id string) (ShardGroupEntry, bool) {
	for _, sg := range s.groups {
		if sg.ID == id {
			return sg, true
		}
	}
	return ShardGroupEntry{}, false
}
