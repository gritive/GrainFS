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
// Task 4c: This path uses fsm.SnapshotCapturedKEKVersion() to select which KEK
// to use for unwrapping. After a KEK rotation, a retained raft snapshot may carry
// DEKs sealed under an older KEK version (e.g. V=1 when the current active is V=2).
// Using store.ActiveKEK() (current active) would fail; we must use the version the
// snapshot trailer explicitly recorded.
//
// Operator-visible failure modes (§7 F#21 / F#22) still apply:
//
//   - KEK missing (F#21): the snapshot-captured KEK version is not present in
//     the keystore. Surfaced as a 3-option remediation message.
//
//   - KEK wrong (F#22): the KEK file exists but does not decrypt the wrapped DEKs
//     (operator replaced it). LoadFromFSM returns an unwrap error wrapped with a
//     scp-from-peer remediation.
//
// When the snapshot has NO wrapped DEKs (PendingDEKVersions returns
// empty), this is a no-op — the fresh keeper from wireDEKKeeper is left
// in place.
func rebuildDEKKeeperFromRestore(state *bootState, fsm *cluster.MetaFSM) error {
	versions, _ := fsm.PendingDEKVersions()
	if len(versions) == 0 {
		return nil
	}

	cfg := nodeconfig.New(state.cfg.DataDir)
	keysDir := cfg.KEKDir()

	snapshotKEKVer := fsm.SnapshotCapturedKEKVersion()
	kek, err := loadKEKForRestore(state, keysDir, snapshotKEKVer)
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

	// cluster.id is required to reconstruct each persisted wrap's
	// DomainDEKFSMWrap AAD. A restore implies prior cluster state, so load
	// strictly (a missing cluster.id is a corrupted node, not a fresh init).
	// Loaded after the KEK so a missing KEK still surfaces the KEK-specific
	// remediation message.
	clusterID, err := cfg.LoadClusterID()
	if err != nil {
		return fmt.Errorf("rebuildDEKKeeperFromRestore: cluster.id: %w", err)
	}

	// The persisted wraps are all sealed under the snapshot's captured active
	// KEK version (applyKEKRotate keeps them in lockstep), so unwrap each under
	// AAD (clusterID, gen, snapshotKEKVer). LoadFromFSM also labels the seal
	// counter with snapshotKEKVer.
	keeper, err := encrypt.LoadFromFSM(kek, clusterID, versions, snapshotKEKVer)
	if err != nil {
		return fmt.Errorf("decrypt FSM-wrapped DEK with KEK at %s: %w. "+
			"This usually means the KEK on this node was rotated or replaced "+
			"and no longer matches the cluster's wrapped DEKs. "+
			"Restore the matching KEK by scp from any healthy peer.", keysDir, err)
	}

	// Bind the FSM's clusterID so a subsequent KEK rotate's verify path uses
	// the SAME DomainDEKFSMWrap AAD the keeper just unwrapped under
	// (Task 1b HIGH 1).
	fsm.SetClusterID(clusterID)
	fsm.SetDEKKeeper(keeper)
	state.dekKeeper = keeper
	state.clusterID = clusterID // keep the single-source clusterID in lockstep with the swapped keeper
	return nil
}

// loadKEKForRestore returns the KEK bytes for the given version. It prefers
// the already-loaded KEKStore on state (populated by wireDEKKeeper in production
// boot) and falls back to loading from keysDir for tests/direct callers.
// Returns a fresh copy of the KEK bytes that the caller must zeroize.
//
// Task 4c: callers pass fsm.SnapshotCapturedKEKVersion() so a retained snapshot
// sealed under an older KEK version is unwrapped with the correct key rather
// than the keystore's current active version.
func loadKEKForRestore(state *bootState, keysDir string, version uint32) ([]byte, error) {
	if state.kekStore != nil {
		return state.kekStore.Get(version)
	}
	if _, err := os.Stat(keysDir); err != nil {
		return nil, err
	}
	store, err := encrypt.LoadOrInitKEKStoreDir(keysDir)
	if err != nil {
		return nil, err
	}
	state.kekStore = store
	return store.Get(version)
}
