//go:build !test_preflight_stub

package server

import (
	"fmt"
	"path/filepath"
)

func preflightCreateDataDirError(dataDir string, err error) error {
	return fmt.Errorf(
		"preflight: cannot create data directory %s: %w\n\n"+
			"Recovery guide:\n"+
			"  1. Ensure the parent directory exists and the process has write permission.\n"+
			"  2. Set --data to a writable path (e.g. --data /var/lib/grainfs).",
		dataDir, err,
	)
}

func preflightDataDirWritableError(dataDir string, err error) error {
	return fmt.Errorf(
		"preflight: data directory %s is not writable: %w\n\n"+
			"Recovery guide:\n"+
			"  1. Check ownership: 'ls -la %s'.\n"+
			"  2. Fix with: 'chown -R $(whoami) %s' or run as the owning user.",
		dataDir, err, filepath.Dir(dataDir), dataDir,
	)
}

func preflightInsufficientDiskError(dataDir string, avail uint64) error {
	return fmt.Errorf(
		"preflight: insufficient disk space at %s: only %s available (need at least 512 MiB)\n\n"+
			"Recovery guide:\n"+
			"  1. Free disk space: 'df -h %s'.\n"+
			"  2. Move the data directory to a larger volume with --data.",
		dataDir, fmtBytes(avail), dataDir,
	)
}

func preflightPortInUseError(addr string, err error) error {
	return fmt.Errorf(
		"preflight: HTTP address %s is already in use: %w\n\n"+
			"Recovery guide:\n"+
			"  1. Find the conflicting process: 'lsof -i %s' or 'ss -tlnp | grep %s'.\n"+
			"  2. Stop the conflicting process, or use a different port with --port.",
		addr, err, addr, addr,
	)
}
