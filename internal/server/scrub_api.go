package server

import (
	"context"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func (s *Server) registerScrubAPI(h *server.Hertz) {
	h.GET(routePathAdminHealthScrub, localhostOnly(), s.scrubStatsHandler)
}

type scrubStatsResponse struct {
	LastRun        time.Time `json:"last_run"`
	ObjectsChecked int64     `json:"objects_checked"`
	ShardErrors    int64     `json:"shard_errors"`
	Repaired       int64     `json:"repaired"`
	Unrepairable   int64     `json:"unrepairable"`
	Available      bool      `json:"available"`
}

func (s *Server) scrubStatsHandler(_ context.Context, c *app.RequestContext) {
	c.JSON(consts.StatusOK, s.scrubStatsSnapshot())
}
