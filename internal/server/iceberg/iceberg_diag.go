package iceberg

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/rs/zerolog/log"
)

// parseIcebergDiagEnv reads two env vars used by the §9.1 iceberg-commits
// instrumentation. Both default to OFF. Parse failures fall back to OFF and
// emit a single zerolog warn (per D5 of the design doc).
//
//	GRAINFS_ICEBERG_ACCESS_LOG=1|true|TRUE  -> access log middleware ON
//	GRAINFS_ICEBERG_COMMIT_TRACE_MS=<ms>    -> slow-commit trace ON above
//	                                           the threshold (0/neg/invalid -> OFF)
func parseIcebergDiagEnv() (accessLog bool, slowThresholdNs int64) {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("GRAINFS_ICEBERG_ACCESS_LOG"))) {
	case "1", "true":
		accessLog = true
	}
	raw := strings.TrimSpace(os.Getenv("GRAINFS_ICEBERG_COMMIT_TRACE_MS"))
	if raw != "" {
		ms, err := strconv.Atoi(raw)
		switch {
		case err != nil:
			log.Warn().Str("env", "GRAINFS_ICEBERG_COMMIT_TRACE_MS").Str("value", raw).
				Msg("iceberg_diag: invalid integer, slow-commit trace disabled")
		case ms > 0:
			slowThresholdNs = int64(ms) * int64(time.Millisecond)
		}
	}
	return accessLog, slowThresholdNs
}

// ApplyDiagEnv stores the parsed flags onto the handler's atomics.
// Called once during NewWithServerStorage boot. Hot paths read via atomic.Load.
func (h *Handler) ApplyDiagEnv() {
	access, slowNs := parseIcebergDiagEnv()
	h.accessLogEnabled.Store(access)
	h.commitSlowThresholdNs.Store(slowNs)
}

// logIcebergAccess emits a single iceberg_access zerolog line.
// Pulled out as a function so it can be unit-tested without building a
// real Hertz RequestContext (zero-value RequestContext is fragile in tests).
func logIcebergAccess(method, path string, status int, elapsed time.Duration) {
	log.Info().
		Str("method", method).
		Str("path", path).
		Int("status", status).
		Float64("elapsed_ms", float64(elapsed.Microseconds())/1000.0).
		Msg("iceberg_access")
}

// icebergAccessLog wraps a Hertz handler with a single-line zerolog emit
// describing each request. When the access log flag is OFF, the closure
// short-circuits with a single atomic.Bool load (zero alloc verified by test).
func (h *Handler) icebergAccessLog(next app.HandlerFunc) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if !h.accessLogEnabled.Load() {
			next(ctx, c)
			return
		}
		start := time.Now()
		next(ctx, c)
		logIcebergAccess(
			string(c.Request.Method()),
			string(c.Request.Path()),
			c.Response.StatusCode(),
			time.Since(start),
		)
	}
}
