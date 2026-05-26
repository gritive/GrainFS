package serveruntime

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/nodeconfig"
)

// wireDEKKeeper loads the cluster KEKStore (keys/<V>.key under dataDir),
// constructs a DEK Keeper from the active KEK, builds the shared
// HandshakeVerifier bound to the persisted cluster.id, and injects the
// keeper into the FSM + registers the DEK post-commit hook.
//
// KEK load mode (§7 B3 / F#21):
//
//   - First-cluster-init mode (NOT joining, NO peers): an empty keys/
//     directory triggers auto-generation of KEK v0 by
//     LoadOrInitKEKStoreDir. This is the very first node bootstrapping
//     its own KEK.
//
//   - Join mode (`--join-pending` OR peers configured): we refuse to
//     auto-generate. A joining node MUST already hold the cluster's KEK
//     before serve can start — auto-generating a fresh random KEK here
//     would produce a node whose KEK never decrypts the cluster's
//     FSM-wrapped DEKs. The pre-flight checks keys/ contents BEFORE
//     calling LoadOrInitKEKStoreDir.
//
// cluster.id (Phase A): generated as UUID v7 on first boot, persisted to
// <dataDir>/cluster.id, and loaded on subsequent boots. Joiners receive
// the file out-of-band (operator scp's it alongside the active KEK).
func wireDEKKeeper(state *bootState, fsm *cluster.MetaFSM) error {
	// Phase A no longer honors GRAINFS_KEK_SOURCE — the keystore is always
	// at <dataDir>/keys/<V>.key (configurable via GRAINFS_KEK_DIR for tests).
	// Surface a clear error if an operator still sets the legacy env var,
	// rather than silently using the default path.
	if v := os.Getenv("GRAINFS_KEK_SOURCE"); v != "" {
		return fmt.Errorf("wireDEKKeeper: GRAINFS_KEK_SOURCE is no longer supported (was: %q). Phase A uses <dataDir>/keys/<V>.key. Unset GRAINFS_KEK_SOURCE and stage your KEK at <dataDir>/keys/0.key (override the directory with GRAINFS_KEK_DIR if needed).", v)
	}

	cfg := nodeconfig.New(state.cfg.DataDir)
	keysDir := cfg.KEKDir()

	// Disk-space pre-flight: refuse boot if the keystore filesystem
	// can't fit a rotation. Probes the parent so we don't fail when the
	// keys/ subdir doesn't exist yet on first boot.
	if err := CheckKeystoreDiskSpace(filepath.Dir(keysDir), MinKeystoreFreeBytes); err != nil {
		return fmt.Errorf("wireDEKKeeper: %w", err)
	}

	// Refuse to auto-generate keys/0.key in any case where this is NOT a
	// fresh-cluster init: join mode (joining peers), OR a restart (prior
	// raft/meta state exists — captured by bootValidateConfig BEFORE
	// bootOpenMetaDB ran). Auto-generation in either case would silently
	// corrupt the cluster — restore would unwrap FSM-stored DEKs with
	// the wrong KEK.
	if state.joinMode || len(state.peers) > 0 || state.priorState {
		if empty, err := encrypt.KeysDirIsEmpty(keysDir); err != nil {
			return fmt.Errorf("wireDEKKeeper: stat keys dir %s: %w", keysDir, err)
		} else if empty {
			return fmt.Errorf("KEK not found at %s and this node is configured to join an existing cluster or has prior raft/meta state. "+
				"A joining or restarting node MUST already hold the cluster's KEK before serve can start — "+
				"auto-generating a fresh KEK here would produce a node whose KEK never decrypts "+
				"the cluster's FSM-wrapped DEKs. Options: "+
				"(a) restore the cluster KEK by scp from any healthy peer "+
				"(the KEK files are identical on every cluster node), "+
				"(b) use `grainfs cluster join <peer>` from the joining node to complete the "+
				"offline-bootstrap handshake (still requires the KEK in place locally), "+
				"(c) decommission this node and rejoin from scratch (loses local raft state).",
				keysDir)
		}
	}

	store, err := encrypt.LoadOrInitKEKStoreDir(keysDir)
	if err != nil {
		return fmt.Errorf("wireDEKKeeper: load keystore %s: %w", keysDir, err)
	}

	activeKEK, err := store.ActiveKEK()
	if err != nil {
		return fmt.Errorf("wireDEKKeeper: active KEK: %w", err)
	}
	defer zeroizeKEKCopy(activeKEK)

	// cluster.id load mode: strict when join mode OR prior state exists,
	// so a missing cluster.id on a restart surfaces as
	// ErrClusterIDMissing rather than silently regenerating a fresh UUID
	// (which would drift the cluster identity from what raft/meta
	// already contains). Loaded BEFORE NewDEKKeeper because every DEK wrap is
	// now AAD-bound to clusterID (DomainDEKFSMWrap).
	var clusterID []byte
	if state.joinMode || len(state.peers) > 0 || state.priorState {
		clusterID, err = cfg.LoadClusterID()
		if err != nil {
			return fmt.Errorf("wireDEKKeeper: cluster.id (strict): %w", err)
		}
	} else {
		clusterID, err = cfg.LoadOrInitClusterID()
		if err != nil {
			return fmt.Errorf("wireDEKKeeper: cluster.id: %w", err)
		}
	}

	keeper, err := encrypt.NewDEKKeeper(activeKEK, clusterID)
	if err != nil {
		return fmt.Errorf("wireDEKKeeper: init DEK keeper: %w", err)
	}
	// Label the seal counter with the keystore's active KEK version so the
	// grainfs_kek_seal_count metric reports against the correct version from
	// boot (a restored node may already be past version 0).
	keeper.SetActiveKEKVersion(store.ActiveVersion())

	// Bind the FSM's clusterID so the KEK-rewrap verify path
	// (verifyRewrappedDEKsAgainstWrapSet) reconstructs the SAME
	// DomainDEKFSMWrap AAD the keeper used to seal each wrap. Without this the
	// FSM's clusterID stays all-zero and every KEK rotate would fail AAD auth
	// on a fresh-boot cluster (Task 1b HIGH 1 — production parity with
	// rebuildDEKKeeperFromRestore).
	fsm.SetClusterID(clusterID)

	fsm.SetDEKKeeper(keeper)
	// Wire the same KEKStore into the FSM. Both MetaKEKRotateCmd Apply and
	// KEKRotationLeader.ProposeKEKRotate read f.KEKStore(); without this the
	// whole Phase B rotate/retire/prune lifecycle fails with "keystore not
	// wired" and GET /v1/encrypt/kek/status reports an empty version list.
	// (Caught by the Task 15 e2e lifecycle suite; unit tests wired the store
	// directly so the boot-path gap was invisible.)
	fsm.SetKEKStore(store)
	// Back the FSM keystore with the on-disk keys dir so MetaKEKRotateCmd
	// persists keys/<V>.key (AddAndPersist) and MetaKEKPruneCmd unlinks it
	// (RemoveAndUnlink). Without this the rotation Apply installs K_new in
	// memory only, so rotated KEKs are lost on restart and prune cannot
	// delete the on-disk file. (Caught by the Task 15 e2e lifecycle suite.)
	fsm.SetKEKDir(keysDir)
	state.dekKeeper = keeper
	state.kekStore = store
	state.handshakeVerifier = encrypt.NewHandshakeVerifier(store, clusterID)
	// kekLeaseTracker counts in-flight KEK consumers per version. Phase B has no
	// runtime acquire sites — Phase D wires them (raft snapshot reader holding
	// K_old during decrypt + InstallSnapshot receiver). LeaseSnapshot RPC returns
	// 0 deterministically in Phase B, which is correct: prune-after-retire only
	// requires lease_count == 0, and there are no consumers to drive it nonzero.
	state.kekLeaseTracker = encrypt.NewKEKLeaseTracker()
	WireDEKPostCommit(fsm, nil /* proposer (§6) */, keeper, nil /* scrubberKick (§6) */)
	return nil
}

func zeroizeKEKCopy(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
