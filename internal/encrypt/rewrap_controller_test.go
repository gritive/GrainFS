package encrypt

import (
	"context"
	"errors"
	"sync"
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
	c.MarkReady()
	require.NoError(t, c.Kick(context.Background(), 0))
}

func TestRewrapController_Kick_CallsLaneWithActiveGen(t *testing.T) {
	k := newTestKeeper(t)
	require.NoError(t, k.Rotate())
	require.NoError(t, k.Rotate()) // active gen 2
	c := NewRewrapController(k)
	l := &fakeLane{name: "fake"}
	c.RegisterLane(l)
	c.MarkReady()
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
	c.MarkReady()
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
	c.MarkReady()
	require.NoError(t, c.Kick(context.Background(), 1)) // oldGen == active
	require.Equal(t, 0, l.calls)
}

// TestRewrapController_ConcurrentRegisterAndKick proves RegisterLane is safe to
// call concurrently with Kick (the boot race: lanes are registered after the
// controller is already reachable by a kick triggered during apply-loop replay).
// Run under -race; the atomic copy-on-write lanes slice must not race.
func TestRewrapController_ConcurrentRegisterAndKick(t *testing.T) {
	k := newTestKeeper(t)
	require.NoError(t, k.Rotate()) // active gen 1
	c := NewRewrapController(k)
	c.MarkReady()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.RegisterLane(&fakeLane{name: "x"}) }()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_ = c.Kick(context.Background(), 0)
		}
	}()
	wg.Wait()
}

func TestKick_RefusesBeforeReady(t *testing.T) {
	k := newTestKeeper(t)
	require.NoError(t, k.Rotate())
	require.NoError(t, k.Rotate()) // active gen 2
	c := NewRewrapController(k)
	c.RegisterLane(&fakeLane{name: "A"})
	err := c.Kick(context.Background(), 1) // MarkReady 안 함
	require.ErrorIs(t, err, errLanesNotReady)
}

func TestKick_RunsAllLanesAndAggregatesWhenReady(t *testing.T) {
	k := newTestKeeper(t)
	require.NoError(t, k.Rotate())
	require.NoError(t, k.Rotate()) // active gen 2
	c := NewRewrapController(k)
	laneA := &fakeLane{name: "A", err: errors.New("A incomplete")}
	laneB := &fakeLane{name: "B"}
	c.RegisterLane(laneA)
	c.RegisterLane(laneB)
	c.MarkReady()
	err := c.Kick(context.Background(), 1)
	require.Error(t, err)
	require.Greater(t, laneB.calls, 0, "lane B must run even though lane A errored")
}

func TestKick_NilWhenReadyAndAllClean(t *testing.T) {
	k := newTestKeeper(t)
	require.NoError(t, k.Rotate())
	require.NoError(t, k.Rotate()) // active gen 2
	c := NewRewrapController(k)
	c.RegisterLane(&fakeLane{name: "A"})
	c.MarkReady()
	require.NoError(t, c.Kick(context.Background(), 1))
}

func TestKick_NilWhenReadyAndNoLanes(t *testing.T) {
	k := newTestKeeper(t)
	require.NoError(t, k.Rotate())
	require.NoError(t, k.Rotate()) // active gen 2
	c := NewRewrapController(k)
	c.MarkReady()
	require.NoError(t, c.Kick(context.Background(), 1))
}
