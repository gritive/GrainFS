package cluster

import (
	"context"
	"testing"

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
