package serveruntime

import (
	"context"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/snapshot"
	"github.com/gritive/GrainFS/internal/storage"
)

// StartAutoSnapshotterWhenReady wires the object-level PITR auto-snapshotter
// once the backend reports ready. Interval/retain are driven by cluster config
// (hot-reloadable). Backends that don't implement storage.Snapshotable are
// skipped silently.
func StartAutoSnapshotterWhenReady(
	ctx context.Context,
	dataDir, walDir string,
	backend storage.Backend,
	cfg *cluster.ClusterConfig,
	enc *encrypt.Encryptor,
	readinessTimeout time.Duration,
) error {
	snapshotable, ok := backend.(storage.Snapshotable)
	if !ok {
		log.Debug().Msg("auto-snapshot skipped: backend does not implement Snapshotable")
		return nil
	}
	if err := waitForSnapshotBackendReady(ctx, snapshotable, readinessTimeout); err != nil {
		return err
	}
	objSnapMgr, err := snapshot.NewManagerWithEncryptor(filepath.Join(dataDir, "snapshots"), snapshotable, walDir, enc)
	if err != nil {
		return err
	}
	as := snapshot.NewAutoSnapshotter(objSnapMgr, cfg, 0)
	as.Start(ctx)
	log.Info().
		Dur("interval", cfg.SnapshotInterval()).
		Int32("retain", cfg.SnapshotRetain()).
		Msg("auto-snapshot enabled (cluster-config driven)")
	return nil
}
