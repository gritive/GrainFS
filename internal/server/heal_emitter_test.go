package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/scrubber"
)

type captureSrvEmitter struct {
	events []scrubber.HealEvent
}

func (c *captureSrvEmitter) Emit(ev scrubber.HealEvent) {
	c.events = append(c.events, ev)
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
	em := newHealEmitter(nil, nil)
	em.Emit(scrubber.NewEvent(scrubber.PhaseStartup, scrubber.OutcomeSuccess))
}
