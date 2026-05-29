package serveruntime

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/wal"
)

// bootLogicalWALOpen opens the logical/PITR WAL. It runs AFTER WaitDEKReady
// because the WAL is DEK-sealed and wal.OpenEncrypted decrypts existing records
// during its open scan (wal.go scanMaxSeq). Its sole consumer is the PITR
// backend wrap (wal.NewBackend) in bootBackendWrap, which runs immediately
// after this phase. The data WAL is opened separately and LATER in
// bootShardService (R-FSM-α reorder); bootLogicalWALOpen does not consume
// state.dataWAL, so the previous "data WAL must precede logical WAL" guard
// (formerly in bootWALAndForwardersPart1) was removed — the ordering it
// asserted was inverted by the reorder.
//
// Outputs: state.wal, state.walDir.
// Cleanup: state.AddCleanup closes the WAL.
func bootLogicalWALOpen(ctx context.Context, state *bootState) error {
	state.walDir = filepath.Join(state.cfg.DataDir, "wal")
	// R1: prefer the gen-aware DEK seam. This phase runs after WaitDEKReady, so
	// when the keeper is present it has an active generation and the 16-byte
	// clusterID is loaded (wireDEKKeeper).
	//
	// GEN-FRAME INVARIANT: the logical/PITR WAL pins its dek_gen at open and
	// rejects later seals at a different gen, so the DEK sealer is correct ONLY
	// while the active DEK gen is 0. R1 defers data-DEK rotation (spec decision
	// #5) and gates encryption.rotate-dek to keep the active gen pinned at 0. A
	// future rotation slice MUST grow per-segment gen framing here before the
	// active gen can advance.
	var sealer wal.RecordSealer
	switch {
	case state.dekKeeper != nil && len(state.clusterID) == 16:
		sealer = storage.NewDEKKeeperAdapter(state.dekKeeper, state.clusterID)
	}
	// wal.OpenEncrypted rejects a nil sealer. The encryption-disabled path
	// (no keeper + no static encryptor → sealer == nil) must use the plaintext
	// wal.Open instead.
	var w *wal.WAL
	var err error
	if sealer != nil {
		w, err = wal.OpenEncrypted(state.walDir, sealer, wal.PITRWALNamespace)
	} else {
		w, err = wal.Open(state.walDir)
	}
	if err != nil {
		return fmt.Errorf("open WAL: %w", err)
	}
	state.wal = w
	state.AddCleanup(func() { w.Close() })
	return nil
}
