package server

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/metrics"
)

// eventQueueSize bounds the in-flight event buffer. When full, incoming events
// are dropped rather than blocking request handlers or spawning unbounded goroutines.
const eventQueueSize = 4096

type eventWorker struct {
	store    *eventstore.Store
	ch       chan eventstore.Event
	done     chan struct{}
	stopOnce sync.Once
}

func newEventWorker(store *eventstore.Store, queueSize int) *eventWorker {
	if queueSize <= 0 {
		queueSize = eventQueueSize
	}
	return &eventWorker{
		store: store,
		ch:    make(chan eventstore.Event, queueSize),
		done:  make(chan struct{}),
	}
}

func (w *eventWorker) start() {
	go func() {
		defer close(w.done)
		for e := range w.ch {
			if err := w.store.Append(e); err != nil {
				log.Error().Err(err).Msg("event append failed")
			}
		}
	}()
}

func (w *eventWorker) stop() {
	w.stopOnce.Do(func() {
		close(w.ch)
	})
	<-w.done
}

func (w *eventWorker) emit(e eventstore.Event) bool {
	select {
	case w.ch <- e:
		return true
	default:
		return false
	}
}

// startEventWorker launches a single background worker that drains s.eventCh and
// appends events to s.evStore. It is called from New() when an event store is
// configured. Callers must not send on s.eventCh before this runs.
func (s *Server) startEventWorker() {
	if s.evStore == nil {
		return
	}
	s.eventWorker = newEventWorker(s.evStore, s.eventQueueSize)
	s.eventWorker.start()
}

// stopEventWorker closes the event channel and waits for the worker to drain.
// Safe to call multiple times and safe to call when the worker was never started.
func (s *Server) stopEventWorker() {
	if s.eventWorker == nil {
		return
	}
	s.eventWorker.stop()
}

// emitEvent enqueues e to the event store via a bounded channel. The send is
// non-blocking: when the queue is full, the event is dropped and counted in
// eventDropsTotal so that request handlers cannot be slowed by BadgerDB write
// latency. Drops are surfaced via the counter rather than slog to avoid log
// spam and potential recursion through the stdlib log adapter.
func (s *Server) emitEvent(e eventstore.Event) {
	if s.eventWorker == nil {
		return
	}
	if !s.eventWorker.emit(e) {
		eventDropsTotal.Add(1)
		metrics.EventQueueDropsTotal.Inc()
	}
}

func (s *Server) streamEventsFromHub(ctx context.Context, c *app.RequestContext, categories ...string) {
	pr, pw := io.Pipe()
	go func() {
		s.hub.WriteSSE(ctx, pw, categories...)
		pw.Close()
	}()
	c.Response.SetBodyStream(pr, -1)
}

func (s *Server) queryStoredEvents(since, until time.Time, limit int, types []string) ([]eventstore.Event, error) {
	return s.evStore.Query(since, until, limit, types)
}
