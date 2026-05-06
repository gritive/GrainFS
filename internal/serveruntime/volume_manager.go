package serveruntime

import (
	"context"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/cache/blockcache"
	"github.com/gritive/GrainFS/internal/resourcewatch"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
	"github.com/gritive/GrainFS/internal/volume/dedup"
)

// VolumeManagerOptions captures cobra-flag-derived inputs for
// BuildVolumeManager.
type VolumeManagerOptions struct {
	DedupEnabled   bool
	BlockCacheSize int64
}

type VolumeRuntimeOptions struct {
	VolumeManagerOptions
	DataDir string
	Backend storage.Backend
}

type VolumeRuntime struct {
	Manager       *volume.Manager
	BlockCache    *blockcache.Cache
	ServerOptions []server.Option
	closeOnce     sync.Once
	closeFn       func() error
	closeErr      error
}

func (r *VolumeRuntime) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		if r.closeFn != nil {
			r.closeErr = r.closeFn()
		}
	})
	return r.closeErr
}

func SetupVolumeRuntime(ctx context.Context, opts VolumeRuntimeOptions) (*VolumeRuntime, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	mgr, cache, dedupDB, err := BuildVolumeManager(opts.VolumeManagerOptions, opts.DataDir, opts.Backend)
	if err != nil {
		return nil, err
	}
	runtime := &VolumeRuntime{
		Manager:       mgr,
		BlockCache:    cache,
		ServerOptions: []server.Option{server.WithVolumeManager(mgr), server.WithBlockCache(cache)},
	}
	if dedupDB != nil {
		dedupEntry := resourcewatch.RegisterDB(resourcewatch.DBCategoryDedup, dedupDB)
		runtime.closeFn = func() error {
			resourcewatch.DeregisterDB(dedupEntry)
			return dedupDB.Close()
		}
	}
	return runtime, nil
}

// BuildVolumeManager creates the shared volume.Manager for the serve path.
// Returns the manager, the block cache (so callers can wire it into other
// consumers), and the optional dedup BadgerDB (nil when dedup is disabled
// or its optional role open failed). When the dedup role open fails with
// a "feature disabled" recovery decision, the manager is returned with
// dedup off and no error so the rest of startup proceeds.
func BuildVolumeManager(opts VolumeManagerOptions, dataDir string, backend storage.Backend) (*volume.Manager, *blockcache.Cache, *badger.DB, error) {
	cache := blockcache.New(opts.BlockCacheSize)
	if opts.BlockCacheSize > 0 {
		log.Info().Int64("bytes", opts.BlockCacheSize).Msg("volume block cache enabled")
	}
	mopts := volume.ManagerOptions{BlockCache: cache}
	if !opts.DedupEnabled {
		return volume.NewManagerWithOptions(backend, mopts), cache, nil, nil
	}
	reg := badgerrole.DefaultRegistry()
	db, decision, err := badgerrole.OpenRole(reg, badgerrole.RoleDedup, badgerrole.PathContext{DataDir: dataDir})
	if err != nil {
		if feature, ok := OptionalRoleDisabled(reg, decision); ok {
			LogOptionalRoleDisabled(badgerrole.RoleDedup, feature, err)
			return volume.NewManagerWithOptions(backend, mopts), cache, nil, nil
		}
		return nil, nil, nil, fmt.Errorf("open dedup db: %w", err)
	}
	mopts.DedupIndex = dedup.NewBadgerIndex(db)
	mgr := volume.NewManagerWithOptions(backend, mopts)
	return mgr, cache, db, nil
}
