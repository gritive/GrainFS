package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeLocalGroups satisfies localBackendLookup. nil entries simulate "group
// not running locally."
type fakeLocalGroups struct {
	backends map[string]*GroupBackend
}

func (f *fakeLocalGroups) Backend(groupID string) *GroupBackend { return f.backends[groupID] }

func TestLocalExecution_ResolveRead_LeaderReturnsLocalBackend(t *testing.T) {
	gb := &GroupBackend{}
	groups := &fakeLocalGroups{backends: map[string]*GroupBackend{"g1": gb}}
	e := NewLocalExecution(groups)
	got, err := e.ResolveRead(context.Background(), RouteTarget{GroupID: "g1", SelfIsLeader: true})
	require.NoError(t, err)
	require.Same(t, gb, got)
}

func TestLocalExecution_ResolveRead_OnlyVoterReturnsLocalBackend(t *testing.T) {
	gb := &GroupBackend{}
	groups := &fakeLocalGroups{backends: map[string]*GroupBackend{"g1": gb}}
	e := NewLocalExecution(groups)
	got, err := e.ResolveRead(context.Background(), RouteTarget{GroupID: "g1", SelfIsOnlyVoter: true, SelfIsVoter: true})
	require.NoError(t, err)
	require.Same(t, gb, got)
}

func TestLocalExecution_ResolveRead_NonVoterSignalsForward(t *testing.T) {
	groups := &fakeLocalGroups{backends: map[string]*GroupBackend{}}
	e := NewLocalExecution(groups)
	got, err := e.ResolveRead(context.Background(), RouteTarget{GroupID: "g1"})
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestLocalExecution_ResolveRead_LeaderButGroupNotLocal(t *testing.T) {
	groups := &fakeLocalGroups{backends: map[string]*GroupBackend{}}
	e := NewLocalExecution(groups)
	got, err := e.ResolveRead(context.Background(), RouteTarget{GroupID: "g1", SelfIsLeader: true})
	require.NoError(t, err)
	require.Nil(t, got)
}

type flippableRaftNode struct {
	leaderSeq []bool
	pos       int
}

func (n *flippableRaftNode) IsLeader() bool {
	if n.pos >= len(n.leaderSeq) {
		// After exhausting the sequence, latch to the final value so polling
		// loops have a stable terminal state.
		if len(n.leaderSeq) == 0 {
			return false
		}
		return n.leaderSeq[len(n.leaderSeq)-1]
	}
	v := n.leaderSeq[n.pos]
	n.pos++
	return v
}

func TestLocalExecution_ResolveWrite_LeaderReturnsLocalBackend(t *testing.T) {
	node := &flippableRaftNode{leaderSeq: []bool{true}}
	gb := newGroupBackendWithRaftForTest(node)
	groups := &fakeLocalGroups{backends: map[string]*GroupBackend{"g1": gb}}
	e := NewLocalExecution(groups)
	got, err := e.ResolveWrite(context.Background(), RouteTarget{GroupID: "g1", SelfIsLeader: true, SelfIsVoter: true})
	require.NoError(t, err)
	require.Same(t, gb, got)
}

func TestLocalExecution_ResolveWrite_NonLeaderSignalsForward(t *testing.T) {
	gb := newGroupBackendWithRaftForTest(&flippableRaftNode{leaderSeq: []bool{false}})
	groups := &fakeLocalGroups{backends: map[string]*GroupBackend{"g1": gb}}
	e := NewLocalExecution(groups)
	got, err := e.ResolveWrite(context.Background(), RouteTarget{GroupID: "g1", SelfIsVoter: true})
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestLocalExecution_ResolveObjectWrite_NonLeaderVoterReturnsLocalBackend(t *testing.T) {
	gb := newGroupBackendWithRaftForTest(&flippableRaftNode{leaderSeq: []bool{false}})
	groups := &fakeLocalGroups{backends: map[string]*GroupBackend{"g1": gb}}
	e := NewLocalExecution(groups)
	got, err := e.ResolveObjectWrite(context.Background(), RouteTarget{GroupID: "g1", SelfIsVoter: true})
	require.NoError(t, err)
	require.Same(t, gb, got)
}

// F3 regression — leadership flipped between route and execute.
func TestLocalExecution_ResolveWrite_LeadershipFlipMidCall(t *testing.T) {
	node := &flippableRaftNode{leaderSeq: []bool{false}}
	gb := newGroupBackendWithRaftForTest(node)
	groups := &fakeLocalGroups{backends: map[string]*GroupBackend{"g1": gb}}
	e := NewLocalExecution(groups)
	got, err := e.ResolveWrite(context.Background(), RouteTarget{GroupID: "g1", SelfIsLeader: true, SelfIsVoter: true})
	require.NoError(t, err)
	require.Nil(t, got, "leadership flip between route and execute MUST signal forward")
}

func TestLocalExecution_ResolveWrite_OnlyVoterAcquiresLeadership(t *testing.T) {
	node := &flippableRaftNode{leaderSeq: []bool{false, false, true}}
	gb := newGroupBackendWithRaftForTest(node)
	groups := &fakeLocalGroups{backends: map[string]*GroupBackend{"g1": gb}}
	e := NewLocalExecution(groups)
	got, err := e.ResolveWrite(context.Background(), RouteTarget{GroupID: "g1", SelfIsOnlyVoter: true, SelfIsVoter: true})
	require.NoError(t, err)
	require.Same(t, gb, got)
}

func TestLocalExecution_ResolveWrite_OnlyVoterTimeout(t *testing.T) {
	node := &flippableRaftNode{leaderSeq: []bool{false}}
	gb := newGroupBackendWithRaftForTest(node)
	groups := &fakeLocalGroups{backends: map[string]*GroupBackend{"g1": gb}}
	e := NewLocalExecution(groups)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	got, err := e.ResolveWrite(ctx, RouteTarget{GroupID: "g1", SelfIsOnlyVoter: true, SelfIsVoter: true})
	require.Error(t, err)
	require.Nil(t, got)
}
