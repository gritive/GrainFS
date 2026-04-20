package server

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/scrubber"
)

func TestHealEmitter_BroadcastsToHealCategoryOnly(t *testing.T) {
	hub := NewHub()
	em := newHealEmitter(hub, nil)

	_, healCh, cancelHeal := hub.Subscribe(healEvCategory)
	defer cancelHeal()

	_, logCh, cancelLog := hub.Subscribe("log")
	defer cancelLog()

	ev := scrubber.NewEvent(scrubber.PhaseReconstruct, scrubber.OutcomeSuccess)
	ev.Bucket = "photos"
	ev.Key = "img.jpg"
	ev.ShardID = 2
	ev.CorrelationID = "corr-1"
	em.Emit(ev)

	select {
	case e := <-healCh:
		assert.Equal(t, healEvCategory, e.Type)
		var got scrubber.HealEvent
		require.NoError(t, json.Unmarshal(e.Data, &got))
		assert.Equal(t, scrubber.PhaseReconstruct, got.Phase)
		assert.Equal(t, "photos", got.Bucket)
		assert.EqualValues(t, 2, got.ShardID)
	case <-time.After(time.Second):
		t.Fatal("heal subscriber did not receive event")
	}

	select {
	case e := <-logCh:
		t.Fatalf("log subscriber received unexpected event type=%s", e.Type)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestHealEmitter_PersistsToEventStore(t *testing.T) {
	var captured []eventstore.Event
	em := newHealEmitter(nil, func(e eventstore.Event) {
		captured = append(captured, e)
	})

	ev := scrubber.NewEvent(scrubber.PhaseDetect, scrubber.OutcomeFailed)
	ev.Bucket = "b"
	ev.Key = "k"
	ev.ShardID = 1
	ev.ErrCode = "missing"
	ev.CorrelationID = "corr-9"
	em.Emit(ev)

	require.Len(t, captured, 1)
	got := captured[0]
	assert.Equal(t, healEvCategory, got.Type)
	assert.Equal(t, "detect", got.Action)
	assert.Equal(t, "b", got.Bucket)
	assert.Equal(t, "missing", got.Metadata["err_code"])
	assert.Equal(t, "corr-9", got.Metadata["correlation_id"])
	assert.EqualValues(t, 1, got.Metadata["shard_id"])
}

func TestHealEmitter_NilHubAndEnqueue_NoPanic(t *testing.T) {
	// Both sinks nil — emitter must remain a safe no-op aside from metrics.
	em := newHealEmitter(nil, nil)
	em.Emit(scrubber.NewEvent(scrubber.PhaseStartup, scrubber.OutcomeSuccess))
}
