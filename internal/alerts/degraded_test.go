package alerts_test

import (
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

	tr.Report(true, "shard_unrepairable", "b/k")
	require.True(t, tr.Degraded())

	// Heal report comes in, but tracker should HOLD degraded until 30s of
	// continuous healthy signals have elapsed (avoids flap on transient
	// recovery).
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

	// Even with sustained healthy reports, hold stays in effect until the
	// flap window expires.
	tr.Report(false, "", "")
	clock.advance(2 * time.Minute)
	tr.Report(false, "", "")
	assert.True(t, tr.Degraded(), "hold remains until flap window cools off")

	// After flap window passes (5+ min since last flap), hold releases and
	// the tracker can exit degraded again.
	clock.advance(6 * time.Minute)
	tr.Report(false, "", "")
	assert.False(t, tr.Degraded(), "after flap window cools off, hold releases")
}

func TestDegradedTracker_StatusSnapshotDoesNotMutate(t *testing.T) {
	tr := alerts.NewDegradedTracker(alerts.DegradedConfig{})
	tr.Report(true, "x", "y")

	s := tr.Status()
	assert.True(t, s.Degraded)
	assert.Equal(t, "x", s.LastReason)
	assert.Equal(t, "y", s.LastResource)

	// Snapshot is a value copy — mutating it must not flip tracker state.
	s.Degraded = false
	assert.True(t, tr.Degraded())
}
