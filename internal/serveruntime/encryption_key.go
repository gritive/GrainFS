package serveruntime

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const bulkCipherFormatFile = "encryption.format"

// bulkCipherFormatVersion is the on-disk at-rest format version this binary
// writes and requires. "2" = static bulk cipher XAES (#566). "3" = DEK
// data-sealing cipher is also XAES — bumped because the DEK ciphertext wire
// format (nonce width) changed and AES-GCM-DEK data is unreadable, so an old
// dir must loud-fail rather than mis-read. "4" = the logical/PITR WAL, packblob,
// and single-node PUT pipeline sealers moved from the static encryptor to the
// gen-aware DEK keeper (R1), so a "3" dir's static-sealed data-plane bytes are
// unreadable by a "4" binary and must loud-fail rather than mis-read. "5" = IAM
// credentials (SA secret keys, BucketUpstream secrets) migrated to the DEK seam
// under DomainIAMCredential AAD (R2). Pre-"5" IAM ciphertext used the static
// *encrypt.Encryptor with raw saID AAD; "5" binary uses the DataEncryptor seam
// with BuildAAD(DomainIAMCredential, clusterID, FieldString(saID), FieldString(accessKey))
// and cannot read pre-"5" IAM bytes. "6" = data-group FSM values and data WAL
// records moved to the gen-aware DEK seam with persisted DEK generation frames.
// "7" = Zero-CA bootstrap/node identity no longer uses the legacy static
// encryption.key as boot glue; node.key.enc must be recoverable through KEK
// generation evidence. "8" = raft v2 Badger log stores are encrypted at rest
// by a node-local raft-store key sealed under the cluster KEK. "9" = encrypted
// data WAL node/shard AEAD namespace separation. Greenfield only.
const bulkCipherFormatVersion = "9"

// EnsureBulkCipherFormat enforces the XAES greenfield boundary at boot. Call it
// ONLY when at-rest encryption is enabled. bulkDataPresent reports whether the
// data dir already holds bulk data sealed by a prior run. Returns a non-nil
// error to refuse boot.
func EnsureBulkCipherFormat(dataDir string, bulkDataPresent bool) error {
	path := filepath.Join(dataDir, bulkCipherFormatFile)
	b, err := os.ReadFile(path)
	switch {
	case err == nil:
		if got := strings.TrimSpace(string(b)); got != bulkCipherFormatVersion {
			return fmt.Errorf("bulk-cipher format %q in %s is not supported by this binary (expected %q); data WAL namespace separation was enabled and in-place upgrade is unsupported — create a new cluster", got, bulkCipherFormatFile, bulkCipherFormatVersion)
		}
		return nil
	case os.IsNotExist(err):
		if bulkDataPresent {
			return fmt.Errorf("data dir %s holds bulk data in a pre-XAES-DEK format; in-place upgrade is not supported — create a new cluster (see CHANGELOG XAES-DEK boundary)", dataDir)
		}
		// The guard runs before preflight creates the data dir, so on a fresh
		// (multi-root) boot dataDir may not exist yet. The dir is about to be
		// used regardless, so create it before stamping the marker.
		if err := os.MkdirAll(dataDir, 0o700); err != nil {
			return fmt.Errorf("create data dir %s: %w", dataDir, err)
		}
		if err := os.WriteFile(path, []byte(bulkCipherFormatVersion), 0o600); err != nil {
			return fmt.Errorf("write %s: %w", bulkCipherFormatFile, err)
		}
		return nil
	default:
		return fmt.Errorf("read %s: %w", bulkCipherFormatFile, err)
	}
}

// dataDirHasEntries reports whether path exists and contains at least one entry.
// Unknown errors are treated conservatively as "has entries" to avoid letting
// a misread upgrade through.
func dataDirHasEntries(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return !os.IsNotExist(err)
	}
	defer f.Close()
	names, err := f.Readdirnames(1)
	if err != nil && err != io.EOF {
		return true // conservative: can't confirm empty → assume present
	}
	return len(names) > 0
}

// BulkDataPresent reports whether the data directories already hold bulk data
// sealed by a prior run — i.e. bytes that a pre-XAES binary may have encrypted
// with the old AES-GCM bulk cipher. Returns true if ANY probed location is
// non-empty. Used by EnsureBulkCipherFormat to refuse an in-place upgrade.
//
// Probed locations (every place at-rest encryption writes bulk bytes):
//   - per data root in dataDirs (fallback [dataDir]):
//   - <root>/shards/  — EC shards, cluster mode (shard_service.go:167)
//   - <root>/data/    — single-node object files + _segments (local.go:128,167)
//   - under the primary dataDir:
//   - blobs/          — packblob, active by default at pack-threshold=65537
//     (boot_phases_backend.go:48)
//   - shared-fsm/     — cluster FSM-state BadgerDB with encrypted values
//     (boot_phases.go:235 bootOpenSharedFSMDB)
//   - raft/raft-v2/   — data-plane raft v2 Badger log/stable/snapshot store
//   - meta_raft/raft-v2/ — meta-raft v2 Badger log/stable/snapshot store
//   - metaDir (= --meta-dir or <dataDir>/meta):
//   - BadgerDB with encrypted values — the earliest encrypted write on any
//     deployment (encrypted_badger.go)
//
// dataDirHasEntries treats any os error other than IsNotExist as "present"
// (refuse), so a permissions glitch never silently allows an upgrade.
func BulkDataPresent(dataDir string, dataDirs []string, metaDir string) bool {
	dirs := dataDirs
	if len(dirs) == 0 {
		dirs = []string{dataDir}
	}
	for _, d := range dirs {
		if dataDirHasEntries(filepath.Join(d, "shards")) {
			return true
		}
		if dataDirHasEntries(filepath.Join(d, "data")) {
			return true
		}
	}
	if dataDirHasEntries(filepath.Join(dataDir, "blobs")) {
		return true
	}
	if dataDirHasEntries(filepath.Join(dataDir, "shared-fsm")) {
		return true
	}
	if dataDirHasEntries(filepath.Join(dataDir, "wal")) {
		return true
	}
	if dataDirHasEntries(filepath.Join(dataDir, "raft", "raft-v2")) {
		return true
	}
	if dataDirHasEntries(filepath.Join(dataDir, "meta_raft", "raft-v2")) {
		return true
	}
	return dataDirHasEntries(metaDir)
}
