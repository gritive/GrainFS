package server

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/metrics"
)

// eventDropsTotal counts events dropped due to full queue. Mirrors
// metrics.EventQueueDropsTotal so tests can assert drops without scraping
// the Prometheus registry.
var eventDropsTotal atomic.Uint64

func (s *Server) registerEventsAPI(h *server.Hertz) {
	h.GET("/api/eventlog", localhostOnly(), s.queryEventLog)
}

func (s *Server) queryEventLog(ctx context.Context, c *app.RequestContext) {
	if s.evStore == nil {
		c.JSON(consts.StatusOK, []eventstore.Event{})
		return
	}

	now := time.Now()

	// since: relative offset in seconds from now (how far back to look). Default 1 hour.
	sinceSec := int64(3600)
	if v := string(c.Query("since")); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			sinceSec = n
		}
	}
	since := now.Add(-time.Duration(sinceSec) * time.Second)

	// until: relative offset in seconds from now (how recent to cut off). Default 0 (now).
	until := now
	if v := string(c.Query("until")); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			until = now.Add(-time.Duration(n) * time.Second)
		}
	}

	// limit: default 200
	limit := 200
	if v := string(c.Query("limit")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}

	// type: comma-separated, default all
	var types []string
	if v := string(c.Query("type")); v != "" {
		for _, t := range strings.Split(v, ",") {
			t = strings.TrimSpace(t)
			if t != "" {
				types = append(types, t)
			}
		}
	}

	events, err := s.evStore.Query(since, until, limit, types)
	if err != nil {
		slog.Error("eventlog query failed", "err", err)
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if events == nil {
		events = []eventstore.Event{}
	}
	c.JSON(consts.StatusOK, events)
}

// eventQueueSize bounds the in-flight event buffer. When full, incoming events
// are dropped rather than blocking request handlers or spawning unbounded goroutines.
const eventQueueSize = 4096

// startEventWorker launches a single background worker that drains s.eventCh and
// appends events to s.evStore. It is called from New() when an event store is
// configured. Callers must not send on s.eventCh before this runs.
func (s *Server) startEventWorker() {
	s.eventCh = make(chan eventstore.Event, eventQueueSize)
	s.eventDone = make(chan struct{})
	go func() {
		defer close(s.eventDone)
		for e := range s.eventCh {
			if err := s.evStore.Append(e); err != nil {
				slog.Error("event append failed", "err", err)
			}
		}
	}()
}

// stopEventWorker closes the event channel and waits for the worker to drain.
// Safe to call multiple times and safe to call when the worker was never started.
func (s *Server) stopEventWorker() {
	if s.eventCh == nil {
		return
	}
	s.eventStopOnce.Do(func() {
		close(s.eventCh)
	})
	<-s.eventDone
}

// emitEvent enqueues e to the event store via a bounded channel. The send is
// non-blocking: when the queue is full, the event is dropped and counted in
// eventDropsTotal so that request handlers cannot be slowed by BadgerDB write
// latency. Drops are surfaced via the counter rather than slog to avoid log
// spam and potential recursion through the stdlib log adapter.
func (s *Server) emitEvent(e eventstore.Event) {
	if s.evStore == nil || s.eventCh == nil {
		return
	}
	select {
	case s.eventCh <- e:
	default:
		eventDropsTotal.Add(1)
		metrics.EventQueueDropsTotal.Inc()
	}
}
