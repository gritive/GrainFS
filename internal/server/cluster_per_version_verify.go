package server

import (
	"context"
	"encoding/json"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// verifyPerVersionCutoverHandler exposes GET /v1/cluster/verify-per-version-cutover
// on the admin Unix socket (S4a). It aggregates verifyPerVersionCutover across
// this node's hosted-group buckets and returns the summed readiness tally as JSON.
//
// An optional ?bucket=<name> query param restricts the scan to a single bucket.
//
// Status code policy:
//
//	verifier not wired (single-node / cluster mode not configured) → 503
//	internal error (e.g., bucket list failed)                       → 500
//	happy path (including all-zero counts)                          → 200 PerVersionCutoverReadiness
func (s *Server) verifyPerVersionCutoverHandler(ctx context.Context, c *app.RequestContext) {
	if s.verifyPerVersionCutoverFn == nil {
		c.JSON(consts.StatusServiceUnavailable, map[string]string{
			"error": "verify-per-version-cutover not available (cluster mode not configured)",
		})
		return
	}
	bucket := string(c.QueryArgs().Peek("bucket"))
	result, err := s.verifyPerVersionCutoverFn(ctx, bucket)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{
			"error": err.Error(),
		})
		return
	}
	data, _ := json.Marshal(result)
	c.Data(consts.StatusOK, "application/json", data)
}
