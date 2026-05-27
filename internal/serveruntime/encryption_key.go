package serveruntime

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// LoadOrCreateEncryptionKey loads a key from file or auto-generates one in the
// data directory when allowed. An explicitly provided missing key path is
// treated as an error, because generating a new key would make existing shards
// unreadable.
func LoadOrCreateEncryptionKey(keyFile, dataDir string, allowAutoGenerate bool) (*encrypt.Encryptor, error) {
	explicitPath := keyFile != ""
	if !explicitPath {
		keyFile = filepath.Join(dataDir, "encryption.key")
	}

	keyData, err := os.ReadFile(keyFile)
	if err == nil {
		log.Info().Str("component", "server").Str("key_file", keyFile).Msg("at-rest encryption enabled")
		return encrypt.NewEncryptor(keyData)
	}

	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read key file: %w", err)
	}
	if explicitPath {
		return nil, fmt.Errorf("encryption key file not found: %s (mount failure?): %w", keyFile, err)
	}
	if !allowAutoGenerate {
		return nil, fmt.Errorf("--encryption-key-file is required for cluster/join mode; refusing to auto-generate node-local key")
	}

	if err := os.MkdirAll(filepath.Dir(keyFile), 0o755); err != nil {
		return nil, fmt.Errorf("create key dir: %w", err)
	}
	keyData = make([]byte, 32)
	if _, err := rand.Read(keyData); err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}
	if err := os.WriteFile(keyFile, keyData, 0o600); err != nil {
		return nil, fmt.Errorf("write key file: %w", err)
	}

	log.Info().Str("component", "server").Str("key_file", keyFile).Msg("at-rest encryption enabled (auto-generated key)")
	return encrypt.NewEncryptor(keyData)
}

const bulkCipherFormatFile = "encryption.format"
const bulkCipherFormatXAES = "2"

// EnsureBulkCipherFormat enforces the XAES greenfield boundary at boot. Call it
// ONLY when at-rest encryption is enabled. bulkDataPresent reports whether the
// data dir already holds bulk data sealed by a prior run. Returns a non-nil
// error to refuse boot.
func EnsureBulkCipherFormat(dataDir string, bulkDataPresent bool) error {
	path := filepath.Join(dataDir, bulkCipherFormatFile)
	b, err := os.ReadFile(path)
	switch {
	case err == nil:
		if got := strings.TrimSpace(string(b)); got != bulkCipherFormatXAES {
			return fmt.Errorf("bulk-cipher format %q in %s is not supported by this binary (expected %q); do not downgrade or cross-upgrade", got, bulkCipherFormatFile, bulkCipherFormatXAES)
		}
		return nil
	case os.IsNotExist(err):
		if bulkDataPresent {
			return fmt.Errorf("data dir %s was encrypted with the pre-XAES (AES-GCM) bulk format; in-place upgrade is not supported — create a new cluster (see CHANGELOG XAES boundary)", dataDir)
		}
		// The guard runs before preflight creates the data dir, so on a fresh
		// (multi-root) boot dataDir may not exist yet. The dir is about to be
		// used regardless, so create it before stamping the marker.
		if err := os.MkdirAll(dataDir, 0o700); err != nil {
			return fmt.Errorf("create data dir %s: %w", dataDir, err)
		}
		if err := os.WriteFile(path, []byte(bulkCipherFormatXAES), 0o600); err != nil {
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
//   - datawal/        — encrypted WAL records (boot_phases_storage_runtime.go:40)
//   - blobs/          — packblob, active by default at pack-threshold=65537
//     (boot_phases_backend.go:48)
//   - shared-fsm/     — cluster FSM-state BadgerDB with encrypted values
//     (boot_phases.go:235 bootOpenSharedFSMDB)
//   - wal/            — logical WAL opened via wal.OpenEncrypted
//     (boot_phases_forwarders.go:44 bootWALAndForwarders)
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
	if dataDirHasEntries(filepath.Join(dataDir, "datawal")) {
		return true
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
	return dataDirHasEntries(metaDir)
}

// AllowAutoGenerateEncryptionKey reports whether a node-local key may be
// auto-generated. Cluster mode (raftAddr set) and pending-join state disable
// auto-generation to avoid creating an unrecoverable mismatch with peers.
func AllowAutoGenerateEncryptionKey(dataDir, raftAddr string) bool {
	if raftAddr != "" {
		return false
	}
	if _, err := os.Stat(filepath.Join(dataDir, JoinPendingFile)); err == nil {
		return false
	}
	return true
}
