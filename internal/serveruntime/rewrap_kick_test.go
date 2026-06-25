package serveruntime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

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

// keeperAtGen2 builds a DEK keeper whose active generation is 2 (retired: {0,1}).
func keeperAtGen2(t *testing.T) *encrypt.DEKKeeper {
	t.Helper()
	k, err := encrypt.NewDEKKeeper(make([]byte, 32), make([]byte, 16))
	require.NoError(t, err)
	require.NoError(t, k.Rotate()) // active gen 1
	require.NoError(t, k.Rotate()) // active gen 2
	return k
}

func TestNewRewrapScrubberKick_RoutesToController(t *testing.T) {
	ctrl := encrypt.NewRewrapController(keeperAtGen1(t))
	lane := &recordingLane{}
	ctrl.RegisterLane(lane)
	ctrl.MarkReady()
	kick := newRewrapScrubberKick(ctrl, "node-A", nil)
	kick(context.Background(), 0) // oldGen 0 < active 1 → lane runs
	require.Equal(t, 1, lane.calls)
}

func TestNewRewrapScrubberKick_LaneErrorDoesNotPanic(t *testing.T) {
	ctrl := encrypt.NewRewrapController(keeperAtGen1(t))
	ctrl.RegisterLane(&recordingLane{err: errors.New("boom")})
	ctrl.MarkReady()
	kick := newRewrapScrubberKick(ctrl, "node-A", nil)
	require.NotPanics(t, func() { kick(context.Background(), 0) })
}

type failOnceLane struct {
	mu    sync.Mutex
	calls int
}

func (f *failOnceLane) Name() string { return "fail-once" }
func (f *failOnceLane) RewrapByGen(_ context.Context, _, _ uint32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls == 1 {
		return errors.New("transient")
	}
	return nil
}

func TestNewRewrapScrubberKick_RetriesTransientLaneFailure(t *testing.T) {
	ctrl := encrypt.NewRewrapController(keeperAtGen1(t))
	ctrl.RegisterLane(&failOnceLane{})
	ctrl.MarkReady()

	reports := make(chan uint32, 1)
	report := func(_ context.Context, _ string, gen, _ uint32) error {
		reports <- gen
		return nil
	}

	kick := newRewrapScrubberKickWithRetry(ctrl, "node-A", report, 10*time.Millisecond, 3)
	kick(context.Background(), 0)

	select {
	case gen := <-reports:
		require.Equal(t, uint32(0), gen)
	case <-time.After(time.Second):
		t.Fatal("expected retry to report completion after transient lane failure")
	}
}

func TestScrubberKick_ReportsFullSweptSetOnlyOnCleanReadyKick(t *testing.T) {
	// clean+ready: Kick==(active,nil), RetiredGensBelow(active)=={0,1} → report called for 0 and 1
	t.Run("clean_ready_reports_full_set", func(t *testing.T) {
		ctrl := encrypt.NewRewrapController(keeperAtGen2(t))
		ctrl.RegisterLane(&recordingLane{}) // no-error lane
		ctrl.MarkReady()

		var mu sync.Mutex
		var reported []uint32
		report := func(_ context.Context, _ string, gen, _ uint32) error {
			mu.Lock()
			reported = append(reported, gen)
			mu.Unlock()
			return nil
		}

		kick := newRewrapScrubberKick(ctrl, "node-X", report)
		kick(context.Background(), 1) // oldGen=1, active=2 → Kick returns nil
		require.ElementsMatch(t, []uint32{0, 1}, reported)
	})

	// not ready (MarkReady not called): Kick returns errLanesNotReady → report not called
	t.Run("not_ready_no_report", func(t *testing.T) {
		ctrl := encrypt.NewRewrapController(keeperAtGen1(t))
		ctrl.RegisterLane(&recordingLane{})
		// intentionally no ctrl.MarkReady()

		called := false
		report := func(_ context.Context, _ string, _, _ uint32) error {
			called = true
			return nil
		}

		kick := newRewrapScrubberKick(ctrl, "node-X", report)
		kick(context.Background(), 0)
		require.False(t, called, "report must not be called when lanes are not ready")
	})

	// lane error: Kick returns non-nil → report not called
	t.Run("lane_error_no_report", func(t *testing.T) {
		ctrl := encrypt.NewRewrapController(keeperAtGen1(t))
		ctrl.RegisterLane(&recordingLane{err: errors.New("disk full")})
		ctrl.MarkReady()

		called := false
		report := func(_ context.Context, _ string, _, _ uint32) error {
			called = true
			return nil
		}

		kick := newRewrapScrubberKick(ctrl, "node-X", report)
		kick(context.Background(), 0)
		require.False(t, called, "report must not be called when a lane returned an error")
	})

	// nil report: clean kick with nil report must not panic
	t.Run("nil_report_no_panic", func(t *testing.T) {
		ctrl := encrypt.NewRewrapController(keeperAtGen1(t))
		ctrl.RegisterLane(&recordingLane{})
		ctrl.MarkReady()
		kick := newRewrapScrubberKick(ctrl, "node-X", nil)
		require.NotPanics(t, func() { kick(context.Background(), 0) })
	})
}
