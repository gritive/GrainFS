package serveruntime

import (
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/storage"
)

// bootSnapshotAndApplyLoop builds the packblob+cachedBackend wrap chain,
// registers the s3-cache invalidator, and fires the distBackend apply loop
// goroutine.
//
// Inputs: state.db, state.distBackend, state.stopApply, cfg.PackThreshold,
// cfg.DataDir.
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
	// state.fsm IS the distBackend's FSM (same instance) so snapshot/restore
	// scope matches the apply loop's scope — both carry the "group-0"
	// keyspace prefix over the shared FSM-state DB (C2 P3). raftv2 owns
	// snapshot lifecycle (M5 PR 29); the v1 SnapshotManager wiring that
	// previously lived here is gone.
	state.fsm = state.distBackend.FSMRef()

	state.cachedBackend = storage.NewCachedBackend(state.distBackend)
	state.distBackend.RegisterCacheInvalidator("s3-cache", cluster.CacheInvalidatorFunc(state.cachedBackend.InvalidateKey))

	go state.distBackend.RunApplyLoop(state.stopApply)
	return nil
}
