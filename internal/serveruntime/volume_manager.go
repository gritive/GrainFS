package serveruntime

import (
	"context"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/cache/blockcache"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
	"github.com/gritive/GrainFS/internal/volume/dedup"
)

// VolumeManagerOptions captures cobra-flag-derived inputs for
// BuildVolumeManager.
type VolumeManagerOptions struct {
	BlockCacheSize int64
}

// BuildVolumeManager creates the shared volume.Manager for the serve path.
// Returns the manager, the block cache (so callers can wire it into other
// consumers), and the optional dedup BadgerDB (nil when its optional role
// open failed). When the dedup role open fails with a "feature disabled"
// recovery decision, the manager is returned with dedup off and no error so
// the rest of startup proceeds.
func BuildVolumeManager(opts VolumeManagerOptions, dataDir string, backend storage.Backend) (*volume.Manager, *blockcache.Cache, *badger.DB, error) {
	cache := blockcache.New(opts.BlockCacheSize)
	if opts.BlockCacheSize > 0 {
		log.Info().Int64("bytes", opts.BlockCacheSize).Msg("volume block cache enabled")
	}
	mopts := volume.ManagerOptions{BlockCache: cache}
	reg := badgerrole.DefaultRegistry()
	db, decision, err := badgerrole.OpenRole(reg, badgerrole.RoleDedup, badgerrole.PathContext{DataDir: dataDir})
	if err != nil {
		if feature, ok := OptionalRoleDisabled(reg, decision); ok {
			LogOptionalRoleDisabled(badgerrole.RoleDedup, feature, err)
			mgr := volume.NewManagerWithOptions(backend, mopts)
			if err := mgr.RecoverOnBoot(context.Background()); err != nil {
				return nil, nil, nil, fmt.Errorf("volume recover on boot: %w", err)
			}
			return mgr, cache, nil, nil
		}
		return nil, nil, nil, fmt.Errorf("open dedup db: %w", err)
	}
	mopts.DedupIndex = dedup.NewBadgerIndex(db)
	mgr := volume.NewManagerWithOptions(backend, mopts)
	if err := mgr.RecoverOnBoot(context.Background()); err != nil {
		return nil, nil, nil, fmt.Errorf("volume recover on boot: %w", err)
	}
	return mgr, cache, db, nil
}
