package serveruntime

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/gritive/GrainFS/internal/transport"
)

// ResolveClusterKey applies the bootstrap conflict resolution rules from
// the cluster-key-rotation spec D10:
//   - Disk wins over flag when both present and differ (warn emitted).
//   - Disk only: use disk silently.
//   - Flag only: use flag, mirror to disk on first boot.
//   - Both empty: returns "" (caller decides solo ephemeral path).
//
// Returns (resolved, warning_message, error). Warning is non-empty when the
// caller should log.Warn the operator about a mismatch.
func ResolveClusterKey(dataDir, flagKey string) (string, string, error) {
	ks := transport.NewKeystore(dataDir)
	diskKey, diskErr := ks.ReadCurrent()
	hasDisk := diskErr == nil

	switch {
	case hasDisk && flagKey != "" && diskKey != flagKey:
		warn := fmt.Sprintf("--cluster-key flag (%d chars) does not match keys.d/current.key (%d chars); disk wins. Reconcile via `cluster rotate-key` or update flag to match disk.", len(flagKey), len(diskKey))
		return diskKey, warn, nil
	case hasDisk:
		return diskKey, "", nil
	case flagKey != "":
		// First boot — mirror to disk so subsequent restarts read from disk.
		if err := ks.WriteCurrent(flagKey); err != nil {
			return "", "", fmt.Errorf("mirror flag to keys.d: %w", err)
		}
		return flagKey, "", nil
	default:
		return "", "", nil
	}
}

// GenerateEphemeralClusterKey returns a random 64-char hex string used as a
// per-process cluster identity in solo mode. The key never leaves this
// process (solo has no peers), so its only purpose is to satisfy the
// transport package's PSK requirement (D6). Returns error so a sandboxed
// or seccomp-restricted environment with no /dev/urandom + no getrandom
// fails cleanly via the caller's error path instead of crashing mid-init.
func GenerateEphemeralClusterKey() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("ephemeral cluster key: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
