package serveruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
)

type recordingLane struct {
	calls int
	err   error
}

func (r *recordingLane) Name() string { return "rec" }
func (r *recordingLane) RewrapByGen(_ context.Context, _, _ uint32) error {
	r.calls++
	return r.err
}

// keeperAtGen1 builds a DEK keeper whose active generation is 1.
func keeperAtGen1(t *testing.T) *encrypt.DEKKeeper {
	t.Helper()
	k, err := encrypt.NewDEKKeeper(make([]byte, 32), make([]byte, 16))
	require.NoError(t, err)
	require.NoError(t, k.Rotate()) // active gen 1
	return k
}

func TestNewRewrapScrubberKick_RoutesToController(t *testing.T) {
	ctrl := encrypt.NewRewrapController(keeperAtGen1(t))
	lane := &recordingLane{}
	ctrl.RegisterLane(lane)
	ctrl.MarkReady()
	kick := newRewrapScrubberKick(ctrl)
	kick(context.Background(), 0) // oldGen 0 < active 1 → lane runs
	require.Equal(t, 1, lane.calls)
}

func TestNewRewrapScrubberKick_LaneErrorDoesNotPanic(t *testing.T) {
	ctrl := encrypt.NewRewrapController(keeperAtGen1(t))
	ctrl.RegisterLane(&recordingLane{err: errors.New("boom")})
	ctrl.MarkReady()
	kick := newRewrapScrubberKick(ctrl)
	require.NotPanics(t, func() { kick(context.Background(), 0) })
}
