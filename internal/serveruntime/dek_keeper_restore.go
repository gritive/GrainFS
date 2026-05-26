package serveruntime

import (
	"errors"
	"fmt"
	"os"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/nodeconfig"
)

// rebuildDEKKeeperFromRestore is the post-Restore / pre-apply-loop callback
// passed to MetaRaft.Start. By contract it runs:
//
//  1. AFTER fsm.Restore — so fsm.PendingDEKVersions() reflects the DKVS
//     trailer (if any) from the latest snapshot.
//  2. BEFORE the apply-loop goroutine is launched — so this is the ONLY safe
//     window to call fsm.SetDEKKeeper without racing apply paths that consume
//     the keeper (DEKRotate, DEKVersionPrune, JWTSigningKeyRotate).
//
// Phase A: the KEKStore loaded by wireDEKKeeper carries the active KEK
// (version 0). This restore path uses store.ActiveKEK() to reconstruct a
// keeper from the wrapped DEKs in the FSM trailer. (Later phases will
// pass the full store down so DEKs wrapped under retired KEK versions
// can still be unwrapped on restore.)
//
// Operator-visible failure modes (§7 F#21 / F#22) still apply:
//
//   - KEK missing (F#21): keys/0.key absent and the snapshot trailer has
//     wrapped DEKs. Surfaced as a 3-option remediation message.
//
//   - KEK wrong (F#22): keys/0.key exists but does not decrypt the
//     wrapped DEKs (operator replaced it). LoadFromFSM returns an
//     unwrap error wrapped with a scp-from-peer remediation.
//
// When the snapshot has NO wrapped DEKs (PendingDEKVersions returns
// empty), this is a no-op — the fresh keeper from wireDEKKeeper is left
// in place.
func rebuildDEKKeeperFromRestore(state *bootState, fsm *cluster.MetaFSM) error {
	versions, _ := fsm.PendingDEKVersions()
	if len(versions) == 0 {
		return nil
	}

	keysDir := nodeconfig.New(state.cfg.DataDir).KEKDir()
	kek, err := loadActiveKEKForRestore(state, keysDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, encrypt.ErrKEKVersionUnknown) {
			return fmt.Errorf("KEK not found at %s and the raft snapshot carries FSM-wrapped DEKs that require it. "+
				"This node cannot decrypt cluster state. Options: "+
				"(a) restore the cluster KEK by scp from any healthy peer "+
				"(the KEK files are identical on every cluster node), "+
				"(b) decommission this node and rejoin from scratch (loses local raft state). "+
				"Underlying error: %w", keysDir, err)
		}
		return fmt.Errorf("load KEK from %s: %w", keysDir, err)
	}
	defer zeroizeKEKCopy(kek)

	keeper, err := encrypt.LoadFromFSM(kek, versions)
	if err != nil {
		return fmt.Errorf("decrypt FSM-wrapped DEK with KEK at %s: %w. "+
			"This usually means the KEK on this node was rotated or replaced "+
			"and no longer matches the cluster's wrapped DEKs. "+
			"Restore the matching KEK by scp from any healthy peer.", keysDir, err)
	}

	fsm.SetDEKKeeper(keeper)
	state.dekKeeper = keeper
	return nil
}

// loadActiveKEKForRestore prefers the already-loaded KEKStore on state
// (populated by wireDEKKeeper in production boot) and falls back to
// loading from keysDir for tests/direct callers. Returns a fresh copy
// of the active KEK bytes that the caller must zeroize.
func loadActiveKEKForRestore(state *bootState, keysDir string) ([]byte, error) {
	if state.kekStore != nil {
		return state.kekStore.ActiveKEK()
	}
	if _, err := os.Stat(keysDir); err != nil {
		return nil, err
	}
	store, err := encrypt.LoadOrInitKEKStoreDir(keysDir)
	if err != nil {
		return nil, err
	}
	state.kekStore = store
	return store.ActiveKEK()
}
