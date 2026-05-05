package admin

import "context"

// GetVlogBreakdown returns the vlog watcher's per-category byte breakdown,
// per-DB consecutive GC failure counters, and the latest startup-smoke
// report. Operators reach this via `GET /v1/resource/vlog/breakdown` to
// answer "which BadgerDB category is dominating vlog right now?". When the
// adapter is not wired (no vlog watcher in the running process), returns
// 404.
func GetVlogBreakdown(ctx context.Context, d *Deps) (VlogBreakdownResp, error) {
	if d.VlogBreakdown == nil {
		return VlogBreakdownResp{}, NewNotFound("vlog breakdown not configured")
	}
	return d.VlogBreakdown.Breakdown()
}
