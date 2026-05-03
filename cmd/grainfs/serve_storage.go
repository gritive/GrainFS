package main

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/cache/blockcache"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
	"github.com/gritive/GrainFS/internal/volume/dedup"
)

// buildVolumeManager creates the shared volume.Manager for the serve path.
func buildVolumeManager(cmd *cobra.Command, dataDir string, backend storage.Backend) (*volume.Manager, *blockcache.Cache, *badger.DB, error) {
	dedupEnabled, _ := cmd.Flags().GetBool("dedup")
	cacheSize, _ := cmd.Flags().GetInt64("block-cache-size")
	cache := blockcache.New(cacheSize)
	if cacheSize > 0 {
		log.Info().Int64("bytes", cacheSize).Msg("volume block cache enabled")
	}
	opts := volume.ManagerOptions{BlockCache: cache}
	if !dedupEnabled {
		return volume.NewManagerWithOptions(backend, opts), cache, nil, nil
	}
	dir := filepath.Join(dataDir, "dedup")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, nil, fmt.Errorf("create dedup dir: %w", err)
	}
	db, err := badger.Open(badgerutil.SmallOptions(dir))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open dedup db: %w", err)
	}
	opts.DedupIndex = dedup.NewBadgerIndex(db)
	mgr := volume.NewManagerWithOptions(backend, opts)
	return mgr, cache, db, nil
}

// loadOrCreateEncryptionKey loads a key from file or auto-generates one in the
// data directory. An explicitly provided missing key path is treated as an
// error, because generating a new key would make existing shards unreadable.
func loadOrCreateEncryptionKey(keyFile, dataDir string) (*encrypt.Encryptor, error) {
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
