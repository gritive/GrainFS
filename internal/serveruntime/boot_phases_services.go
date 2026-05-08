package serveruntime

import (
	"path/filepath"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/packblob"
)

// bootSnapshotAndApplyLoop wires the meta-FSM snapshot manager onto the
// distributed backend, restores any prior snapshot, builds the
// packblob+cachedBackend wrap chain, registers the s3-cache invalidator, and
// fires the distBackend apply loop goroutine.
//
// Inputs: state.db, state.logStore, state.node, state.distBackend,
// state.stopApply, cfg.PackThreshold, cfg.DataDir.
//
// Outputs: state.fsm, state.snapMgr, state.cachedBackend.
//
// Phase ordering rationale: snapshot Restore must precede the apply-loop go
// statement; otherwise applies between Restore() and SetSnapshotManager would
// be dropped from the snapshot. The s3-cache invalidator MUST register before
// the apply loop goroutine starts — otherwise FSM-replicated writes can land
// before invalidator wiring and stale cache entries survive cross-node.
func bootSnapshotAndApplyLoop(state *bootState) error {
	state.fsm = cluster.NewFSM(state.db)
	state.snapMgr = raft.NewSnapshotManager(state.logStore, state.fsm, raft.SnapshotConfig{Threshold: 10000})
	state.distBackend.SetSnapshotManager(state.snapMgr, state.node)

	snapIdx, err := state.snapMgr.Restore()
	if err != nil {
		log.Warn().Err(err).Msg("snapshot restore failed")
	} else if snapIdx > 0 {
		log.Info().Uint64("index", snapIdx).Msg("restored from snapshot")
	}

	// Wrapping chain (inner → outer): distBackend → packblob → cachedBackend.
	// The WAL + pullthrough wrappers are added downstream in run.go (later
	// services phases) because they depend on the WAL handle and IAMStore
	// which are not yet phase-owned.
	var inner storage.Backend = state.distBackend
	if state.cfg.PackThreshold > 0 {
		blobDir := filepath.Join(state.cfg.DataDir, "blobs")
		pb, perr := packblob.NewPackedBackend(inner, blobDir, int64(state.cfg.PackThreshold))
		if perr != nil {
			return perr
		}
		inner = pb
		log.Info().Int("threshold", state.cfg.PackThreshold).Msg("packed blob storage enabled")
	}

	state.cachedBackend = storage.NewCachedBackend(inner)
	state.distBackend.RegisterCacheInvalidator("s3-cache", cluster.CacheInvalidatorFunc(state.cachedBackend.InvalidateKey))

	go state.distBackend.RunApplyLoop(state.stopApply)
	return nil
}
