package serveruntime

import (
	"context"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/snapshot"
	"github.com/gritive/GrainFS/internal/storage"
)

// StartAutoSnapshotterWhenReady wires the object-level PITR auto-snapshotter
// once the backend reports ready. interval<=0 disables. Backends that don't
// implement storage.Snapshotable are skipped silently.
//
// readinessTimeout caps how long this waits for the backend to be listable
// before returning the underlying error — long enough for cold restart-replay
// to finish, short enough that a stuck backend surfaces.
func StartAutoSnapshotterWhenReady(
	ctx context.Context,
	dataDir, walDir string,
	backend storage.Backend,
	interval time.Duration,
	retain int,
	readinessTimeout time.Duration,
) error {
	if interval <= 0 {
		return nil
	}
	snapshotable, ok := backend.(storage.Snapshotable)
	if !ok {
		log.Debug().Msg("auto-snapshot skipped: backend does not implement Snapshotable")
		return nil
	}
	if err := waitForSnapshotBackendReady(ctx, snapshotable, readinessTimeout); err != nil {
		return err
	}
	objSnapMgr, err := snapshot.NewManager(filepath.Join(dataDir, "snapshots"), snapshotable, walDir)
	if err != nil {
		return err
	}
	as := snapshot.NewAutoSnapshotter(objSnapMgr, interval, retain)
	as.Start(ctx)
	log.Info().Dur("interval", interval).Int("retain", retain).Msg("auto-snapshot enabled")
	return nil
}
