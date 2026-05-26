package serveruntime

import (
	"errors"
	"fmt"
	"syscall"
)

// ErrKeystoreDiskLow indicates the filesystem holding the keystore has
// less free space than the configured minimum. Boot refuses rather than
// risking a partial KEK write during the first rotation Apply.
var ErrKeystoreDiskLow = errors.New("keystore directory has insufficient free disk space")

// MinKeystoreFreeBytes is the conservative startup threshold (64 KiB).
// This is enough for thousands of 32-byte KEK files plus filesystem
// overhead. A real disk-full hazard during a Phase B rotation Apply
// would trip this well in advance of the actual write failure, giving
// the operator time to free space.
const MinKeystoreFreeBytes uint64 = 64 * 1024

// CheckKeystoreDiskSpace probes the filesystem holding probeDir and
// returns ErrKeystoreDiskLow if free bytes < minFreeBytes. Returns a
// different (non-wrapped) error if the path cannot be statfs'd —
// typically a misconfigured dataDir.
//
// Phase A: synchronous startup check only. Phase B may add a periodic
// monitor + Prometheus-style metric export under the same package.
//
// Note: probeDir does not have to be the keystore directory itself —
// statfs measures the filesystem, so the dataDir parent is a fine
// probe target if the keystore subdir doesn't exist yet on first boot.
func CheckKeystoreDiskSpace(probeDir string, minFreeBytes uint64) error {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(probeDir, &stat); err != nil {
		return fmt.Errorf("statfs %q: %w", probeDir, err)
	}
	// Bavail = free blocks available to unprivileged users; safer than
	// Bfree which includes reserved blocks.
	free := uint64(stat.Bsize) * uint64(stat.Bavail)
	if free < minFreeBytes {
		return fmt.Errorf("%w: free=%d bytes, want >= %d (%s)", ErrKeystoreDiskLow, free, minFreeBytes, probeDir)
	}
	return nil
}
