package server

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/eventstore"
)

func (s *Server) registerEventsAPI(h *server.Hertz) {
	h.GET("/api/eventlog", s.queryEventLog)
}

func (s *Server) queryEventLog(ctx context.Context, c *app.RequestContext) {
	if s.evStore == nil {
		c.JSON(consts.StatusOK, []eventstore.Event{})
		return
	}

	now := time.Now()

	// since: Unix seconds, default 1 hour ago
	sinceSec := int64(3600)
	if v := string(c.Query("since")); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			sinceSec = n
		}
	}
	since := time.Unix(sinceSec, 0)
	if sinceSec <= 3600 { // treat as relative offset
		since = now.Add(-time.Duration(sinceSec) * time.Second)
	}

	// until: Unix seconds, default now
	until := now
	if v := string(c.Query("until")); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 3600 {
			until = time.Unix(n, 0)
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

// emitEvent sends an event to the store in a fire-and-forget goroutine.
func (s *Server) emitEvent(e eventstore.Event) {
	if s.evStore == nil {
		return
	}
	go func() {
		if err := s.evStore.Append(e); err != nil {
			slog.Error("event append failed", "err", err)
		}
	}()
}
