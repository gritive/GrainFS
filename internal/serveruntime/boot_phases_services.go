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
// Outputs: state.fsm, state.snapMgr (nil in v2 mode), state.cachedBackend.
//
// Phase ordering rationale: snapshot Restore must precede the apply-loop go
// statement; otherwise applies between Restore() and SetSnapshotManager would
// be dropped from the snapshot. The s3-cache invalidator MUST register before
// the apply loop goroutine starts — otherwise FSM-replicated writes can land
// before invalidator wiring and stale cache entries survive cross-node.
//
// v2 mode (GRAINFS_RAFT_V2=serveruntime): raftv2 owns snapshot lifecycle
// internally via SnapshotStore + CreateSnapshot + InstallSnapshot. The v1
// raft.SnapshotManager would require *raft.Node and *raft.BadgerLogStore
// which the v2 path does not produce. We skip the SnapshotManager wiring;
// DistributedBackend.RunApplyLoop already nil-checks snapMgr (backend.go
// line ~472) so apply-loop traffic continues unaffected. RaftSnapshotStatus
// / TriggerRaftSnapshot will return "unavailable" under v2 — an accepted
// staging limitation tracked in TODOS.md until v2 snapshot triggers are
// surfaced through the RaftNode interface in a future PR.
func bootSnapshotAndApplyLoop(state *bootState) error {
	state.fsm = cluster.NewFSM(state.db)
	if v1Node, ok := state.node.(*raft.Node); ok {
		state.snapMgr = raft.NewSnapshotManager(state.logStore, state.fsm, raft.SnapshotConfig{Threshold: 10000})
		state.distBackend.SetSnapshotManager(state.snapMgr, v1Node)

		snapIdx, err := state.snapMgr.Restore()
		if err != nil {
			log.Warn().Err(err).Msg("snapshot restore failed")
		} else if snapIdx > 0 {
			log.Info().Uint64("index", snapIdx).Msg("restored from snapshot")
		}
	} else {
		// v2 path: SnapshotManager is v1-only. raftv2 handles snapshots
		// natively (see internal/raft/v2/snapshot_badger.go); the apply
		// loop's nil-check tolerates state.snapMgr == nil.
		log.Info().Msg("raft v2: skipping v1 SnapshotManager (v2 handles snapshots internally)")
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
