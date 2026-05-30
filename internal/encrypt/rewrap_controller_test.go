package encrypt

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeLane struct {
	name      string
	calls     int
	gotOld    uint32
	gotActive uint32
	err       error
}

func (f *fakeLane) Name() string { return f.name }
func (f *fakeLane) RewrapByGen(_ context.Context, oldGen, activeGen uint32) error {
	f.calls++
	f.gotOld, f.gotActive = oldGen, activeGen
	return f.err
}

func TestRewrapController_ZeroLanes_NoOp(t *testing.T) {
	k := newTestKeeper(t)
	require.NoError(t, k.Rotate()) // active gen 1
	c := NewRewrapController(k)
	require.NoError(t, c.Kick(context.Background(), 0))
}

func TestRewrapController_Kick_CallsLaneWithActiveGen(t *testing.T) {
	k := newTestKeeper(t)
	require.NoError(t, k.Rotate())
	require.NoError(t, k.Rotate()) // active gen 2
	c := NewRewrapController(k)
	l := &fakeLane{name: "fake"}
	c.RegisterLane(l)
	require.NoError(t, c.Kick(context.Background(), 1))
	require.Equal(t, 1, l.calls)
	require.Equal(t, uint32(1), l.gotOld)
	require.Equal(t, uint32(2), l.gotActive)
}

func TestRewrapController_Kick_PropagatesLaneError(t *testing.T) {
	k := newTestKeeper(t)
	require.NoError(t, k.Rotate())
	require.NoError(t, k.Rotate())
	c := NewRewrapController(k)
	c.RegisterLane(&fakeLane{name: "boom", err: errors.New("disk full")})
	err := c.Kick(context.Background(), 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "disk full")
}

func TestRewrapController_Kick_OldGenNotBelowActive_NoOp(t *testing.T) {
	k := newTestKeeper(t)
	require.NoError(t, k.Rotate()) // active gen 1
	c := NewRewrapController(k)
	l := &fakeLane{name: "fake"}
	c.RegisterLane(l)
	require.NoError(t, c.Kick(context.Background(), 1)) // oldGen == active
	require.Equal(t, 0, l.calls)
}
