package server

import (
	"context"
	"encoding/json"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// cacheStatus handles GET /api/cache/status. Reports the volume block
// cache hit/miss counters + resident bytes so the operations dashboard
// can show whether the cache is doing useful work and how full it is.
func (s *Server) cacheStatus(ctx context.Context, c *app.RequestContext) {
	resp := map[string]any{
		"block_cache": map[string]any{
			"enabled": false,
		},
		"shard_cache": map[string]any{
			"enabled": false,
		},
	}
	if s.blockCache != nil {
		stats := s.blockCache.Stats()
		hitRate := 0.0
		if stats.Hits+stats.Misses > 0 {
			hitRate = 100 * float64(stats.Hits) / float64(stats.Hits+stats.Misses)
		}
		resp["block_cache"] = map[string]any{
			"enabled":        stats.CapacityByte > 0,
			"hits":           stats.Hits,
			"misses":         stats.Misses,
			"evictions":      stats.Evictions,
			"resident_bytes": stats.ResidentByte,
			"capacity_bytes": stats.CapacityByte,
			"hit_rate_pct":   hitRate,
		}
	}
	if s.shardCache != nil {
		stats := s.shardCache.Stats()
		hitRate := 0.0
		if stats.Hits+stats.Misses > 0 {
			hitRate = 100 * float64(stats.Hits) / float64(stats.Hits+stats.Misses)
		}
		resp["shard_cache"] = map[string]any{
			"enabled":        stats.CapacityByte > 0,
			"hits":           stats.Hits,
			"misses":         stats.Misses,
			"evictions":      stats.Evictions,
			"resident_bytes": stats.ResidentByte,
			"capacity_bytes": stats.CapacityByte,
			"hit_rate_pct":   hitRate,
		}
	}
	data, _ := json.Marshal(resp)
	c.Data(consts.StatusOK, "application/json", data)
}
