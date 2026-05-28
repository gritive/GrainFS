package serveruntime

import (
	"github.com/gritive/GrainFS/internal/storage"
)

// wireIAMEncryptor swaps the live DEKKeeperAdapter into BOTH the IAM applier
// and the IAM admin API (R2). The admin API has its own DataEncryptor field
// independent of the Applier's — missing either path would silently use the
// wrong encryptor for new credentials (codex P0).
//
// MUST be called AFTER state.dekKeeper is at its final value (after any
// restore-time reassignment in dek_keeper_restore.go) AND BEFORE the first
// IAM apply / admin write. The meta-raft apply loop is gated by WaitDEKReady,
// so apply paths cannot run before this fires.
//
// Idempotent — safe to call from both the fresh-boot path (immediately after
// wireDEKKeeper) and the restore path (after the keeper is rebuilt from
// snapshot, which may swap the pointer). SetEncryptor stores via
// atomic.Pointer.Store, so last write wins.
//
// No-op when any required dep is missing (test configs, IAM not wired,
// keeper not built — none of these are reachable in production).
func wireIAMEncryptor(state *bootState) {
	if state == nil || state.dekKeeper == nil {
		return
	}
	adapter := storage.NewDEKKeeperAdapter(state.dekKeeper, state.clusterID)
	if state.cfg.IAMApplier != nil {
		state.cfg.IAMApplier.SetEncryptor(adapter)
	}
	if state.iamAdminAPI != nil {
		state.iamAdminAPI.SetEncryptor(adapter)
	}
}
