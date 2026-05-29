package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// fakeEvacuator records EvacuateVoter calls and returns scripted errors.
type fakeEvacuator struct {
	mu    sync.Mutex
	calls []evacCall
	errs  map[string]error // key: groupID
}
type evacCall struct{ groupID, revoked, replacement string }

func (m *fakeEvacuator) EvacuateVoter(_ context.Context, groupID, revoked, replacement string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, evacCall{groupID, revoked, replacement})
	if m.errs != nil {
		if err, ok := m.errs[groupID]; ok {
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

func TestEvacuator_MovesRevokedVoterToHealthyPeer_OnLedGroup(t *testing.T) {
	ev := newDataGroupEvacuatorForTest("node-a", revokedSet("node-b"), &fakeEvacuator{},
		func() []evacTarget {
			return []evacTarget{{groupID: "group-7", revokedNode: "node-b", peerIDs: []string{"node-a", "node-b", "node-c"}}}
		},
		func(string, map[string]struct{}) (string, bool) { return "node-d", true },
	)
	ev.reconcileOnce(context.Background())
	require.Equal(t, []evacCall{{"group-7", "node-b", "node-d"}}, ev.mover.(*fakeEvacuator).calls)
}

func TestEvacuator_ShrinksWhenNoHealthyCandidate(t *testing.T) {
	ev := newDataGroupEvacuatorForTest("node-a", revokedSet("node-b"), &fakeEvacuator{},
		func() []evacTarget {
			return []evacTarget{{groupID: "group-7", revokedNode: "node-b", peerIDs: []string{"node-a", "node-b"}}}
		},
		func(string, map[string]struct{}) (string, bool) { return "", false },
	)
	ev.reconcileOnce(context.Background())
	require.Equal(t, []evacCall{{"group-7", "node-b", ""}}, ev.mover.(*fakeEvacuator).calls)
}

func TestEvacuator_NoLedTargets_NoOp(t *testing.T) {
	ev := newDataGroupEvacuatorForTest("node-a", revokedSet("node-b"), &fakeEvacuator{},
		func() []evacTarget { return nil },
		func(string, map[string]struct{}) (string, bool) { return "", false },
	)
	ev.reconcileOnce(context.Background())
	require.Empty(t, ev.mover.(*fakeEvacuator).calls)
}

func TestEvacuator_LeadershipTransferred_IsNotFatal(t *testing.T) {
	ev := newDataGroupEvacuatorForTest("node-a", revokedSet("node-b"),
		&fakeEvacuator{errs: map[string]error{"group-7": ErrLeadershipTransferred}},
		func() []evacTarget {
			return []evacTarget{{groupID: "group-7", revokedNode: "node-b", peerIDs: []string{"node-a", "node-b", "node-c"}}}
		},
		func(string, map[string]struct{}) (string, bool) { return "node-d", true },
	)
	ev.reconcileOnce(context.Background())
	require.Len(t, ev.mover.(*fakeEvacuator).calls, 1)
}

func TestPickHealthyExcluding_SkipsExcludedAndPicksLightest(t *testing.T) {
	loads := map[string]float64{"a": 70, "b": 10, "c": 30, "d": 5}
	loadFn := func(id string) (float64, bool) {
		l, ok := loads[id]
		return l, ok
	}
	candidates := []string{"a", "b", "c", "d"}

	// "d" is the lightest (5) but excluded; "b" (10) is the lightest non-excluded.
	got, ok := PickHealthyExcluding(candidates, loadFn, revokedSet("d"))
	require.True(t, ok)
	require.Equal(t, "b", got)

	// All excluded → ("", false) to signal a shrink.
	_, ok = PickHealthyExcluding(candidates, loadFn, revokedSet("a", "b", "c", "d"))
	require.False(t, ok)
}

func TestEvacuator_DoesNotMoveToAnotherRevokedNode(t *testing.T) {
	var capturedExclude map[string]struct{}
	ev := newDataGroupEvacuatorForTest("node-a", revokedSet("node-b", "node-d"), &fakeEvacuator{},
		func() []evacTarget {
			return []evacTarget{{groupID: "group-7", revokedNode: "node-b", peerIDs: []string{"node-a", "node-b", "node-c"}}}
		},
		func(_ string, exclude map[string]struct{}) (string, bool) {
			capturedExclude = exclude
			return "", false
		},
	)
	ev.reconcileOnce(context.Background())
	_, hasB := capturedExclude["node-b"]
	_, hasD := capturedExclude["node-d"]
	require.True(t, hasB, "revoked target must be excluded")
	require.True(t, hasD, "other revoked nodes must be excluded as candidates")
}

// newDataGroupEvacuatorForTest builds an evacuator with injected derivation
// functions and a tiny tick. Test-only.
func newDataGroupEvacuatorForTest(
	localNodeID string,
	revoked map[string]struct{},
	mover voterMover,
	ledTargets func() []evacTarget,
	pickHealthy func(string, map[string]struct{}) (string, bool),
) *DataGroupEvacuator {
	return &DataGroupEvacuator{
		localNodeID: localNodeID,
		src:         staticRevocationSource{revoked: revoked},
		mover:       mover,
		logger:      log.With().Str("component", "evacuator-test").Logger(),
		ledTargets:  ledTargets,
		pickHealthy: pickHealthy,
		tick:        time.Millisecond,
		wakeCh:      make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
	}
}

type staticRevocationSource struct{ revoked map[string]struct{} }

func (s staticRevocationSource) RevokedNodeIDs() map[string]struct{} { return s.revoked }
