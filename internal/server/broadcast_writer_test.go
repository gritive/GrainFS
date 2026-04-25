package server

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBroadcastWriter_Write_NoSubscribers verifies that Write short-circuits
// when there are no active SSE subscribers (HasSubscribers() == false).
func TestBroadcastWriter_Write_NoSubscribers(t *testing.T) {
	hub := NewHub()
	bw := &broadcastWriter{hub: hub}

	payload := []byte(`{"level":"info","msg":"hello"}`)
	n, err := bw.Write(payload)
	require.NoError(t, err)
	assert.Equal(t, len(payload), n)

	// No subscriber → nothing should have been broadcast.
	assert.False(t, hub.HasSubscribers())
}

// TestBroadcastWriter_Write_WithSubscriber verifies that Write fans the payload
// to an active SSE subscriber.
func TestBroadcastWriter_Write_WithSubscriber(t *testing.T) {
	hub := NewHub()
	bw := &broadcastWriter{hub: hub}

	_, ch, cancel := hub.Subscribe()
	defer cancel()

	payload := []byte(`{"level":"info","msg":"hello"}`)
	n, err := bw.Write(payload)
	require.NoError(t, err)
	assert.Equal(t, len(payload), n)

	select {
	case e := <-ch:
		assert.Equal(t, "log", e.Type)
		assert.Equal(t, payload, e.Data)
	case <-time.After(time.Second):
		t.Fatal("subscriber did not receive log event")
	}
}

// TestBroadcastWriter_WriteLevel_InfoAndAbove verifies that Info and above
// are forwarded through Write.
func TestBroadcastWriter_WriteLevel_InfoAndAbove(t *testing.T) {
	hub := NewHub()
	bw := &broadcastWriter{hub: hub}

	_, ch, cancel := hub.Subscribe()
	defer cancel()

	for _, level := range []zerolog.Level{zerolog.InfoLevel, zerolog.WarnLevel, zerolog.ErrorLevel, zerolog.FatalLevel} {
		payload := []byte(`{"level":"` + level.String() + `"}`)
		n, err := bw.WriteLevel(level, payload)
		require.NoError(t, err, "level=%s", level)
		assert.Equal(t, len(payload), n)

		select {
		case e := <-ch:
			assert.Equal(t, "log", e.Type)
		case <-time.After(time.Second):
			t.Fatalf("WriteLevel(%s) did not forward to subscriber", level)
		}
	}
}

// TestBroadcastWriter_WriteLevel_BelowInfo verifies that Debug/Trace are
// silently dropped (security: unauthenticated /api/events must not expose debug logs).
func TestBroadcastWriter_WriteLevel_BelowInfo(t *testing.T) {
	hub := NewHub()
	bw := &broadcastWriter{hub: hub}

	_, ch, cancel := hub.Subscribe()
	defer cancel()

	for _, level := range []zerolog.Level{zerolog.DebugLevel, zerolog.TraceLevel} {
		payload := []byte(`{"level":"` + level.String() + `"}`)
		n, err := bw.WriteLevel(level, payload)
		require.NoError(t, err, "level=%s", level)
		assert.Equal(t, len(payload), n)
	}

	// Nothing should have been queued.
	select {
	case e := <-ch:
		t.Fatalf("expected no event for debug/trace, got type=%s", e.Type)
	case <-time.After(50 * time.Millisecond):
		// correct — nothing arrived
	}
}

// TestBroadcastWriter_Write_CopyIsolation verifies that the broadcast payload
// is a defensive copy: mutating the original slice after Write must not affect
// the already-queued event data.
func TestBroadcastWriter_Write_CopyIsolation(t *testing.T) {
	hub := NewHub()
	bw := &broadcastWriter{hub: hub}

	_, ch, cancel := hub.Subscribe()
	defer cancel()

	payload := []byte(`{"msg":"original"}`)
	_, err := bw.Write(payload)
	require.NoError(t, err)

	// Mutate original after write.
	copy(payload, "XXXXXXXXXXXXXXXXXX")

	select {
	case e := <-ch:
		assert.Equal(t, `{"msg":"original"}`, string(e.Data), "broadcast data should be an independent copy")
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

// TestHub_HasSubscribers_CounterAccuracy verifies that HasSubscribers() reflects
// Subscribe and cancel calls accurately (the atomic counter must be wired).
func TestHub_HasSubscribers_CounterAccuracy(t *testing.T) {
	hub := NewHub()
	assert.False(t, hub.HasSubscribers(), "fresh hub should have no subscribers")

	_, _, cancel1 := hub.Subscribe()
	assert.True(t, hub.HasSubscribers(), "one subscriber → HasSubscribers should be true")

	_, _, cancel2 := hub.Subscribe()
	assert.True(t, hub.HasSubscribers())

	cancel1()
	assert.True(t, hub.HasSubscribers(), "one subscriber remaining")

	cancel2()
	assert.False(t, hub.HasSubscribers(), "all cancelled → HasSubscribers should be false")
}

// TestHub_HasSubscribers_DoubleCancelSafe verifies that calling cancel()
// more than once does not under-count active subscribers.
func TestHub_HasSubscribers_DoubleCancelSafe(t *testing.T) {
	hub := NewHub()

	_, _, cancel := hub.Subscribe()
	cancel()
	cancel() // idempotent: LoadAndDelete on missing key is a no-op

	assert.False(t, hub.HasSubscribers())
}
