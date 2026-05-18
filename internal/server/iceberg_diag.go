package server

import (
	"os"
	"strconv"
	"strings"
	"time"

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

// applyIcebergDiagEnv stores the parsed flags onto the server's atomics.
// Called once during NewWithServerStorage boot. Hot paths read via atomic.Load.
func (s *Server) applyIcebergDiagEnv() {
	access, slowNs := parseIcebergDiagEnv()
	s.icebergAccessLogEnabled.Store(access)
	s.icebergCommitSlowThresholdNs.Store(slowNs)
}
