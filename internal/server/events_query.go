package server

import (
	"strconv"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
)

type eventLogQuery struct {
	Since time.Time
	Until time.Time
	Limit int
	Types []string
}

func parseEventLogQuery(c *app.RequestContext, now time.Time) eventLogQuery {
	sinceSec := int64(3600)
	if v := string(c.Query("since")); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			sinceSec = n
		}
	}

	until := now
	if v := string(c.Query("until")); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			until = now.Add(-time.Duration(n) * time.Second)
		}
	}

	limit := 200
	if v := string(c.Query("limit")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}

	return eventLogQuery{
		Since: now.Add(-time.Duration(sinceSec) * time.Second),
		Until: until,
		Limit: limit,
		Types: parseEventLogTypes(string(c.Query("type"))),
	}
}

func parseEventLogTypes(raw string) []string {
	if raw == "" {
		return nil
	}
	var types []string
	for _, t := range strings.Split(raw, ",") {
		t = strings.TrimSpace(t)
		if t != "" {
			types = append(types, t)
		}
	}
	return types
}
