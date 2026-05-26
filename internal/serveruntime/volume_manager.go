package serveruntime

import (
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cache/blockcache"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
)

// VolumeManagerOptions captures cobra-flag-derived inputs for
// BuildVolumeManager.
type VolumeManagerOptions struct {
	BlockCacheSize int64
}

// BuildVolumeManager creates the shared volume.Manager for the serve path.
// Returns the manager and the block cache (so callers can wire it into other
// consumers).
func BuildVolumeManager(opts VolumeManagerOptions, dataDir string, backend storage.Backend) (*volume.Manager, *blockcache.Cache, error) {
	cache := blockcache.New(opts.BlockCacheSize)
	if opts.BlockCacheSize > 0 {
		log.Info().Int64("bytes", opts.BlockCacheSize).Msg("volume block cache enabled")
	}
	mgr := volume.NewManagerWithOptions(backend, volume.ManagerOptions{BlockCache: cache})
	return mgr, cache, nil
}
