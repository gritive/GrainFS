//go:build test_admin_endpoints

package serveruntime

import (
	"context"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	hzserver "github.com/cloudwego/hertz/pkg/app/server"

	"github.com/gritive/GrainFS/internal/cluster"
)

// registerTestEndpoints wires PUT /test/node_stats onto the admin UDS server.
// Only compiled when the test_admin_endpoints build tag is active — never in
// production binaries. The E2E harness (Task 13) uses this to inject per-node
// disk/RPS stats so placement decisions are observable without real gossip.
func registerTestEndpoints(h *hzserver.Hertz, store *cluster.NodeStatsStore) {
	if store == nil {
		// Balancer disabled: register a stub that returns 503 so callers get a
		// clear signal rather than a silent no-op or a nil-panic.
		h.PUT("/test/node_stats", func(c context.Context, ctx *app.RequestContext) {
			ctx.JSON(503, map[string]string{"error": "balancer disabled; placement stats store unavailable"})
		})
		return
	}
	h.PUT("/test/node_stats", func(c context.Context, ctx *app.RequestContext) {
		nodeID := string(ctx.QueryArgs().Peek("nodeID"))
		if nodeID == "" {
			ctx.JSON(400, map[string]string{"error": "missing nodeID"})
			return
		}
		disk, _ := strconv.ParseUint(string(ctx.QueryArgs().Peek("disk")), 10, 64)
		rps, _ := strconv.ParseFloat(string(ctx.QueryArgs().Peek("rps")), 64)
		store.Set(cluster.NodeStats{
			NodeID:         nodeID,
			DiskAvailBytes: disk,
			RequestsPerSec: rps,
		})
		ctx.SetStatusCode(204)
	})
}
