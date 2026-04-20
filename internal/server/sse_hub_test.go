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
	hub.clients.Range(func(_, _ any) bool {
		count++
		return true
	})
	require.Equal(t, 0, count)
}
