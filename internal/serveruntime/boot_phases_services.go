package serveruntime

import (
	"path/filepath"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/packblob"
)

// bootSnapshotAndApplyLoop builds the packblob+cachedBackend wrap chain,
// registers the s3-cache invalidator, and fires the distBackend apply loop
// goroutine.
//
// Inputs: state.db, state.node, state.distBackend, state.stopApply,
// cfg.PackThreshold, cfg.DataDir.
//
// Outputs: state.fsm, state.cachedBackend.
//
// As of M5 PR 29 v2 is the only raft engine; raftv2 owns snapshot lifecycle
// internally via SnapshotStore + CreateSnapshot + InstallSnapshot
// (internal/raft/v2/snapshot_badger.go). The v1 raft.SnapshotManager is no
// longer wired — DistributedBackend.RaftSnapshotStatus /
// TriggerRaftSnapshot route through RaftNode.SnapshotStatus /
// RaftNode.CreateSnapshot directly.
//
// The s3-cache invalidator MUST register before the apply loop goroutine
// starts — otherwise FSM-replicated writes can land before invalidator
// wiring and stale cache entries survive cross-node.
func bootSnapshotAndApplyLoop(state *bootState) error {
	state.fsm = cluster.NewFSM(state.db, nil)

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
