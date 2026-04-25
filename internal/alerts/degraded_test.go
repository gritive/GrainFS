package alerts_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/alerts"
)

func TestDegradedTracker_EntryImmediate(t *testing.T) {
	clock := newFakeClock(time.Unix(0, 0))
	tr := alerts.NewDegradedTracker(alerts.DegradedConfig{
		ExitStableWindow: 30 * time.Second,
		FlapWindow:       5 * time.Minute,
		FlapThreshold:    3,
		Clock:            clock.Now,
	})
	defer tr.Stop()

	require.False(t, tr.Degraded(), "tracker starts healthy")

	tr.Report(true, "shard_unrepairable", "b/k")
	assert.True(t, tr.Degraded(), "report=true must enter degraded immediately")
}

func TestDegradedTracker_ExitRequiresStableWindow(t *testing.T) {
	clock := newFakeClock(time.Unix(0, 0))
	tr := alerts.NewDegradedTracker(alerts.DegradedConfig{
		ExitStableWindow: 30 * time.Second,
		FlapWindow:       5 * time.Minute,
		FlapThreshold:    3,
		Clock:            clock.Now,
	})
	defer tr.Stop()

	tr.Report(true, "shard_unrepairable", "b/k")
	require.True(t, tr.Degraded())

	tr.Report(false, "", "")
	clock.advance(10 * time.Second)
	assert.True(t, tr.Degraded(), "must remain degraded inside 30s stability window")

	clock.advance(25 * time.Second) // total 35s healthy
	tr.Report(false, "", "")
	assert.False(t, tr.Degraded(), "after 30s stable healthy, must exit degraded")
}

func TestDegradedTracker_ExitResetsOnNewFault(t *testing.T) {
	clock := newFakeClock(time.Unix(0, 0))
	tr := alerts.NewDegradedTracker(alerts.DegradedConfig{
		ExitStableWindow: 30 * time.Second,
		FlapWindow:       5 * time.Minute,
		FlapThreshold:    3,
		Clock:            clock.Now,
	})
	defer tr.Stop()

	tr.Report(true, "shard_unrepairable", "b/k")
	tr.Report(false, "", "")
	clock.advance(20 * time.Second)
	tr.Report(true, "shard_unrepairable", "b/k") // fault came back inside window
	clock.advance(40 * time.Second)
	tr.Report(false, "", "")
	assert.True(t, tr.Degraded(), "fault reset the stability clock; still degraded")
}

func TestDegradedTracker_FlapCounterHoldsAfterThreshold(t *testing.T) {
	clock := newFakeClock(time.Unix(0, 0))

	var heldCalls int
	tr := alerts.NewDegradedTracker(alerts.DegradedConfig{
		ExitStableWindow: 1 * time.Second,
		FlapWindow:       5 * time.Minute,
		FlapThreshold:    3,
		Clock:            clock.Now,
		OnHold: func(reason string) {
			heldCalls++
			assert.Contains(t, reason, "flap")
		},
	})
	defer tr.Stop()

	// 3 fault→heal cycles inside the 5-min window must trigger HOLD mode.
	for i := 0; i < 3; i++ {
		tr.Report(true, "shard_unrepairable", "b/k")
		tr.Report(false, "", "")
		clock.advance(2 * time.Second) // exit window passes
		tr.Report(false, "", "")
		clock.advance(10 * time.Second) // gap before next flap
	}

	assert.True(t, tr.Degraded(), "after 3 flaps in window, tracker holds in degraded")
	assert.Equal(t, 1, heldCalls, "OnHold callback fires exactly once on threshold cross")

	tr.Report(false, "", "")
	clock.advance(2 * time.Minute)
	tr.Report(false, "", "")
	assert.True(t, tr.Degraded(), "hold remains until flap window cools off")

	clock.advance(6 * time.Minute)
	tr.Report(false, "", "")
	assert.False(t, tr.Degraded(), "after flap window cools off, hold releases")
}

// TestDegradedTracker_OnStateChangeFiresOnTransition verifies OnStateChange is
// invoked exactly on degraded↔healthy transitions, not on repeated same-state
// reports.
func TestDegradedTracker_OnStateChangeFiresOnTransition(t *testing.T) {
	clock := newFakeClock(time.Unix(0, 0))
	var calls []bool
	tr := alerts.NewDegradedTracker(alerts.DegradedConfig{
		FlapWindow:    1 * time.Second,
		FlapThreshold: 99,
		Clock:         clock.Now,
		OnStateChange: func(degraded bool) { calls = append(calls, degraded) },
	})
	defer tr.Stop()

	require.Empty(t, calls, "fresh tracker fires no callback")

	tr.Report(true, "shard_unrepairable", "b/k")
	require.Equal(t, []bool{true}, calls, "enter degraded fires callback(true)")

	tr.Report(true, "shard_unrepairable", "b/k")
	require.Equal(t, []bool{true}, calls, "stay degraded = no callback")

	tr.Report(false, "", "")
	require.Equal(t, []bool{true, false}, calls, "exit degraded fires callback(false)")

	tr.Report(false, "", "")
	require.Equal(t, []bool{true, false}, calls, "stay healthy = no callback")
}

// TestDegradedTracker_OnStateChangeSerializedWithStatus proves that OnStateChange
// runs in the actor goroutine and is serialized with Status(): while the callback
// is executing, a concurrent Status() call blocks until the callback returns.
// This guarantees gauge mirrors stay consistent with tracker state (no observable
// gauge/tracker divergence window).
func TestDegradedTracker_OnStateChangeSerializedWithStatus(t *testing.T) {
	clock := newFakeClock(time.Unix(0, 0))

	peerSawLock := make(chan struct{})
	callbackReturning := make(chan struct{})

	var peerCompleted atomic.Bool
	var tr *alerts.DegradedTracker
	tr = alerts.NewDegradedTracker(alerts.DegradedConfig{
		FlapWindow:    1 * time.Second,
		FlapThreshold: 99,
		Clock:         clock.Now,
		OnStateChange: func(bool) {
			// Peer goroutine races to send a Status() query to the actor.
			// Since the actor is busy running this callback, Status() blocks
			// until the callback returns — the actor processes one message at a time.
			peerStarted := make(chan struct{})
			go func() {
				close(peerStarted)
				_ = tr.Status()
				peerCompleted.Store(true)
				close(peerSawLock)
			}()
			<-peerStarted
			time.Sleep(30 * time.Millisecond)
			require.False(t, peerCompleted.Load(),
				"Status() must block while OnStateChange runs (actor serializes both)")
			close(callbackReturning)
		},
	})
	defer tr.Stop()

	tr.Report(true, "x", "y")

	<-callbackReturning
	select {
	case <-peerSawLock:
		// peer completed AFTER callback returned — correct
	case <-time.After(time.Second):
		t.Fatal("peer Status() never returned after callback finished")
	}
	require.True(t, peerCompleted.Load())
}

func TestDegradedTracker_StatusSnapshotDoesNotMutate(t *testing.T) {
	tr := alerts.NewDegradedTracker(alerts.DegradedConfig{})
	defer tr.Stop()
	tr.Report(true, "x", "y")

	s := tr.Status()
	assert.True(t, s.Degraded)
	assert.Equal(t, "x", s.LastReason)
	assert.Equal(t, "y", s.LastResource)

	// Snapshot is a value copy — mutating it must not flip tracker state.
	s.Degraded = false
	assert.True(t, tr.Degraded())
}

func TestDegradedTracker_StopIdempotent(t *testing.T) {
	tr := alerts.NewDegradedTracker(alerts.DegradedConfig{})
	tr.Stop()
	tr.Stop() // must not panic
}
