package server

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHub_SubscribeReceivesBroadcast(t *testing.T) {
	hub := NewHub()

	_, ch, cancel := hub.Subscribe()
	defer cancel()

	go hub.Broadcast(Event{Type: "metric", Data: []byte(`{"v":1}`)})

	select {
	case e := <-ch:
		assert.Equal(t, "metric", e.Type)
		assert.Equal(t, []byte(`{"v":1}`), e.Data)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestHub_MultipleSubscribers(t *testing.T) {
	hub := NewHub()
	const n = 3

	channels := make([]<-chan Event, n)
	cancels := make([]func(), n)
	for i := range n {
		_, channels[i], cancels[i] = hub.Subscribe()
		defer cancels[i]()
	}

	go hub.Broadcast(Event{Type: "log", Data: []byte(`"hello"`)})

	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		ch := channels[i]
		go func() {
			defer wg.Done()
			select {
			case e := <-ch:
				assert.Equal(t, "log", e.Type)
			case <-time.After(time.Second):
				t.Errorf("subscriber timed out")
			}
		}()
	}
	wg.Wait()
}

func TestHub_CancelRemovesSubscriber(t *testing.T) {
	hub := NewHub()

	_, _, cancel := hub.Subscribe()
	cancel()

	// After cancel, no clients should be registered.
	count := 0
	hub.clients.Range(func(_ string, _ *subscriber) bool {
		count++
		return true
	})
	require.Equal(t, 0, count)
}

func TestHub_CategoryFilter_OnlyMatchingDelivered(t *testing.T) {
	hub := NewHub()

	_, healCh, cancelHeal := hub.Subscribe("heal")
	defer cancelHeal()

	hub.Broadcast(Event{Type: "log", Data: []byte(`"ignored"`)})
	hub.Broadcast(Event{Type: "heal", Data: []byte(`{"phase":"detect"}`)})

	select {
	case e := <-healCh:
		assert.Equal(t, "heal", e.Type)
	case <-time.After(time.Second):
		t.Fatal("heal subscriber did not receive heal event")
	}

	// log event must NOT have been queued for heal subscriber.
	select {
	case e := <-healCh:
		t.Fatalf("heal subscriber received unexpected event type=%s", e.Type)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestHub_NoCategory_ReceivesAll(t *testing.T) {
	// Backward compat: callers that pass no categories must receive every event.
	hub := NewHub()

	_, ch, cancel := hub.Subscribe()
	defer cancel()

	hub.Broadcast(Event{Type: "log", Data: []byte("a")})
	hub.Broadcast(Event{Type: "heal", Data: []byte("b")})
	hub.Broadcast(Event{Type: "metric", Data: []byte("c")})

	got := map[string]bool{}
	for range 3 {
		select {
		case e := <-ch:
			got[e.Type] = true
		case <-time.After(time.Second):
			t.Fatalf("timed out, got=%v", got)
		}
	}
	assert.True(t, got["log"] && got["heal"] && got["metric"])
}

func TestHub_MultipleCategories(t *testing.T) {
	hub := NewHub()

	_, ch, cancel := hub.Subscribe("heal", "metric")
	defer cancel()

	hub.Broadcast(Event{Type: "log", Data: []byte("ignored")})
	hub.Broadcast(Event{Type: "heal", Data: []byte("h")})
	hub.Broadcast(Event{Type: "metric", Data: []byte("m")})

	got := map[string]bool{}
	for range 2 {
		select {
		case e := <-ch:
			got[e.Type] = true
		case <-time.After(time.Second):
			t.Fatalf("timeout, got=%v", got)
		}
	}
	assert.True(t, got["heal"] && got["metric"])
}
