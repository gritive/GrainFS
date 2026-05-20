package serveruntime

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"

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
