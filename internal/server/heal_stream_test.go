package server

import (
	"bufio"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/scrubber"
)

// TestHub_WriteSSE_HealCategoryOnly drives Hub.WriteSSE end-to-end through an
// io.Pipe, mirroring how the /api/events/heal/stream handler streams events to
// browser EventSource clients. We assert that the on-the-wire format is the
// expected SSE framing (`event: heal\ndata: {...}\n\n`) and that log events do
// not bleed into the heal stream.
func TestHub_WriteSSE_HealCategoryOnly(t *testing.T) {
	hub := NewHub()
	em := newHealEmitter(hub, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pr, pw := io.Pipe()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		hub.WriteSSE(ctx, pw, healEvCategory)
		pw.Close()
	}()

	// Give the subscriber a moment to register before broadcasting.
	time.Sleep(20 * time.Millisecond)

	// Noise on a different category should not appear on the heal stream.
	hub.Broadcast(Event{Type: "log", Data: []byte(`"ignored"`)})

	healEv := scrubber.NewEvent(scrubber.PhaseWrite, scrubber.OutcomeSuccess)
	healEv.Bucket = "photos"
	healEv.Key = "img.jpg"
	healEv.ShardID = 3
	healEv.CorrelationID = "corr-stream"
	em.Emit(healEv)

	reader := bufio.NewReader(pr)
	var (
		eventLine string
		dataLine  string
	)

	deadline := time.After(2 * time.Second)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimRight(line, "\n")
			switch {
			case strings.HasPrefix(line, "event: "):
				eventLine = line
			case strings.HasPrefix(line, "data: "):
				dataLine = line
				return
			}
		}
	}()

	select {
	case <-done:
	case <-deadline:
		cancel()
		<-done
		t.Fatalf("timed out waiting for SSE frame, got event=%q data=%q", eventLine, dataLine)
	}

	cancel()
	pw.Close()
	wg.Wait()

	require.Equal(t, "event: heal", eventLine, "wrong category in SSE frame")
	assert.Contains(t, dataLine, `"phase":"write"`)
	assert.Contains(t, dataLine, `"bucket":"photos"`)
	assert.Contains(t, dataLine, `"correlation_id":"corr-stream"`)
}

// TestHealEmitter_DoesNotBlockOnSlowSubscriber asserts the iron rule from
// Phase 13: scrubber emit latency must be bounded even if a dashboard tab is
// frozen. The Hub uses non-blocking sends, so a subscriber that never reads
// must not stall the emitter.
func TestHealEmitter_DoesNotBlockOnSlowSubscriber(t *testing.T) {
	hub := NewHub()
	em := newHealEmitter(hub, nil)

	// Subscribe but never read — channel buffer is 8 (sse_hub.go).
	_, _, cancel := hub.Subscribe(healEvCategory)
	defer cancel()

	done := make(chan struct{})
	go func() {
		for i := range 100 {
			ev := scrubber.NewEvent(scrubber.PhaseDetect, scrubber.OutcomeFailed)
			ev.ShardID = int32(i)
			em.Emit(ev)
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("emitter blocked on slow subscriber — Hub.Broadcast must be non-blocking")
	}
}
