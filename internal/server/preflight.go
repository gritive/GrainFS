//go:build !test_preflight_stub

package server

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"

	"github.com/rs/zerolog/log"
)

const (
	// warnDiskBytes: log a warning when available disk drops below this.
	warnDiskBytes uint64 = 2 * 1024 * 1024 * 1024 // 2 GiB
	// failDiskBytes: refuse to boot when available disk is below this.
	failDiskBytes uint64 = 512 * 1024 * 1024 // 512 MiB
)

// PreflightConfig carries the arguments for RunSystemPreflight.
type PreflightConfig struct {
	DataDir  string // must be writable
	HTTPAddr string // e.g. ":9000" — checked for port conflicts
}

// RunSystemPreflight runs boot-time environment checks and returns an error
// when conditions are severe enough to prevent safe operation. Warnings are
// logged but do not block startup.
//
// Checks performed:
//  1. Data directory: exists and is writable.
//  2. Disk space: at least 512 MiB free; warns below 2 GiB.
//  3. HTTP port: not already bound by another process.
func RunSystemPreflight(cfg PreflightConfig) error {
	if err := checkDataDir(cfg.DataDir); err != nil {
		return err
	}
	if err := checkDiskSpace(cfg.DataDir); err != nil {
		return err
	}
	if err := checkPortFree(cfg.HTTPAddr); err != nil {
		return err
	}
	return nil
}

// checkDataDir verifies the data directory exists and can be written to.
func checkDataDir(dataDir string) error {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return preflightCreateDataDirError(dataDir, err)
	}

	probe := filepath.Join(dataDir, ".grainfs-preflight")
	if err := os.WriteFile(probe, []byte("ok"), 0o600); err != nil {
		return preflightDataDirWritableError(dataDir, err)
	}
	_ = os.Remove(probe)
	return nil
}

// checkDiskSpace verifies sufficient disk space is available for dataDir.
func checkDiskSpace(dataDir string) error {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dataDir, &stat); err != nil {
		// Non-fatal: we can't always stat (e.g. unsupported FS in CI).
		log.Warn().Err(err).Str("dir", dataDir).Msg("preflight: disk space check skipped (stat failed)")
		return nil
	}
	avail := stat.Bavail * uint64(stat.Bsize) //nolint:gosec
	switch {
	case avail < failDiskBytes:
		return preflightInsufficientDiskError(dataDir, avail)
	case avail < warnDiskBytes:
		log.Warn().
			Str("dir", dataDir).
			Str("available", fmtBytes(avail)).
			Msg("preflight: low disk space — consider freeing space or expanding the volume")
	}
	return nil
}

// checkPortFree verifies the HTTP port is not already in use.
func checkPortFree(addr string) error {
	if addr == "" {
		return nil
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return preflightPortInUseError(addr, err)
	}
	_ = l.Close()
	return nil
}

func fmtBytes(b uint64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20))
	default:
		return fmt.Sprintf("%d KiB", b>>10)
	}
}
