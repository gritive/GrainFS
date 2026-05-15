package server

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/eventstore"
)

// eventDropsTotal counts events dropped due to full queue. Mirrors
// metrics.EventQueueDropsTotal so tests can assert drops without scraping
// the Prometheus registry.
var eventDropsTotal atomic.Uint64

func (s *Server) registerEventsAPI(h *server.Hertz) {
	h.GET(routePathEventLog, localhostOnly(), s.queryEventLog)
	h.GET(routePathEvents, s.streamEvents)
	h.GET(routePathHealEventsStream, s.streamHealEvents)
}

func (s *Server) streamEvents(ctx context.Context, c *app.RequestContext) {
	s.streamHubEvents(ctx, c)
}

func (s *Server) streamHealEvents(ctx context.Context, c *app.RequestContext) {
	s.streamHubEvents(ctx, c, healEvCategory)
}

func (s *Server) streamHubEvents(ctx context.Context, c *app.RequestContext, categories ...string) {
	c.Response.Header.Set("Content-Type", "text/event-stream")
	c.Response.Header.Set("Cache-Control", "no-cache")
	c.Response.Header.Set("Connection", "keep-alive")
	c.SetStatusCode(consts.StatusOK)
	s.streamEventsFromHub(ctx, c, categories...)
}

func (s *Server) queryEventLog(ctx context.Context, c *app.RequestContext) {
	if !s.routeFeatureAvailable(routeFeatureEventLog) {
		c.JSON(consts.StatusOK, []eventstore.Event{})
		return
	}

	q := parseEventLogQuery(c, time.Now())
	events, err := s.queryStoredEvents(q.Since, q.Until, q.Limit, q.Types)
	if err != nil {
		log.Error().Err(err).Msg("eventlog query failed")
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if events == nil {
		events = []eventstore.Event{}
	}
	c.JSON(consts.StatusOK, events)
}
