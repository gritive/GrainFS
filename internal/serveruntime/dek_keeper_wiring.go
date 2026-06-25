package serveruntime

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/nodeconfig"
	"github.com/gritive/GrainFS/internal/storage/packblob"
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
// isGenesisBoot reports whether this node is bootstrapping a brand-new cluster
// as the initial single voter — the sole source of truth for genesis material
// (KEK v0, cluster.id, DEK gen-0). It is the negation of "joiner or restart":
// joinMode (.join-pending), static peers configured, or prior raft/meta state.
// SAME signal the KEK auto-generate branch and the cluster.id strict-mode branch
// already trust.
func isGenesisBoot(state *bootState) bool {
	return !(state.inviteJoinMode || len(state.peers) > 0 || state.priorState)
}

func loadKEKStoreAndClusterID(state *bootState) error {
	if state.kekStore != nil && len(state.clusterID) == 16 {
		return nil
	}
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
	if !isGenesisBoot(state) {
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

	protector, err := buildKEKProtector(state.cfg)
	if err != nil {
		return fmt.Errorf("wireDEKKeeper: %w", err)
	}
	store, err := encrypt.LoadOrInitKEKStoreDirWithProtector(keysDir, protector)
	if err != nil {
		return fmt.Errorf("wireDEKKeeper: load keystore %s: %w", keysDir, err)
	}
	if protector.Name() != "plaintext" {
		log.Printf("wireDEKKeeper: at-rest KEK protector active: %s", protector.Name())
	}

	// cluster.id load mode: strict when join mode OR prior state exists,
	// so a missing cluster.id on a restart surfaces as
	// ErrClusterIDMissing rather than silently regenerating a fresh UUID
	// (which would drift the cluster identity from what raft/meta
	// already contains). Loaded BEFORE NewDEKKeeper because every DEK wrap is
	// now AAD-bound to clusterID (DomainDEKFSMWrap).
	var clusterID []byte
	if !isGenesisBoot(state) {
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
	state.kekStore = store
	state.clusterID = clusterID
	return nil
}

func wireDEKKeeper(state *bootState, fsm *cluster.MetaFSM) error {
	if err := loadKEKStoreAndClusterID(state); err != nil {
		return err
	}

	store := state.kekStore
	clusterID := state.clusterID
	keysDir := nodeconfig.New(state.cfg.DataDir).KEKDir()
	activeKEK, err := store.ActiveKEK()
	if err != nil {
		return fmt.Errorf("wireDEKKeeper: active KEK: %w", err)
	}
	defer zeroizeKEKCopy(activeKEK)

	// Genesis vs non-genesis DEK keeper (Phase D Task 5). Only the genesis node
	// (fresh single-voter init) is the sole source of truth for gen-0 — it
	// generates a random gen-0 locally and replicates it via the ungated
	// bootstrap propose. Joiners / restarts start EMPTY: gen-0 arrives via raft
	// log replay or snapshot restore (rebuildDEKKeeperFromRestore via
	// LoadFromFSM). Generating DEK material locally on a non-genesis node would
	// diverge across the cluster.
	var keeper *encrypt.DEKKeeper
	if isGenesisBoot(state) {
		keeper, err = encrypt.NewDEKKeeper(activeKEK, clusterID) // random gen-0, AAD-bound
	} else {
		keeper, err = encrypt.NewEmptyDEKKeeper(activeKEK, clusterID) // gen-0 arrives via replay/restore
	}
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
	state.handshakeVerifier = encrypt.NewHandshakeVerifier(store, clusterID)
	// Single source for the data-plane AAD clusterID (slice C). bootShardService
	// and bootOwnedGroupsAndEC read this so the EC-shard WRITE and READ
	// (ShardService) bind the SAME clusterID — divergence fails every GET.
	state.clusterID = clusterID
	// kekLeaseTracker counts in-flight KEK consumers per version. Phase B has no
	// runtime acquire sites — Phase D wires them (raft snapshot reader holding
	// K_old during decrypt + InstallSnapshot receiver). LeaseSnapshot RPC returns
	// 0 deterministically in Phase B, which is correct: prune-after-retire only
	// requires lease_count == 0, and there are no consumers to drive it nonzero.
	state.kekLeaseTracker = encrypt.NewKEKLeaseTracker()
	// Wire the real DEK proposer (state.metaRaft) and leader gate now that
	// metaRaft is already stored on state (boot_phases_raft.go:54 runs before
	// this call at line 100). The leader gate collapses N-node post-commit
	// fan-out to a single proposal.
	//
	// Unit tests that call wireDEKKeeper directly without a MetaRaft pass nil
	// for both proposer and isLeader; nil isLeader is treated as not-leader
	// (fail-safe) by DEKPostCommitDispatcher.handleConfigPut.
	var isLeaderFn func() bool
	if state.metaRaft != nil {
		isLeaderFn = state.metaRaft.IsLeader
	}
	// S6a/S6b: wire the rewrap controller behind scrubberKick now (this phase
	// runs before the backends exist), but register the real data-rewrap lanes
	// LATER in wireRewrapLanes — once state.distBackend / state.packedBackend are
	// constructed (bootOwnedGroupsAndEC / bootBackendWrap run after this). The
	// scrubberKick closure captures the SAME controller, so lanes registered
	// post-boot are seen by every post-rotation kick. Migration-only (no
	// completion reporting or prune yet).
	rewrapCtrl := encrypt.NewRewrapController(keeper)
	state.rewrapController = rewrapCtrl
	// Wire the completion reporter: each data-holder node self-proposes its own
	// completion (ProposeDEKRewrapProgress) after a clean Kick. nil metaRaft is
	// treated as nil report (fail-safe for unit tests that boot without a raft).
	var reportFn func(ctx context.Context, nodeID string, gen, epoch uint32) error
	if state.metaRaft != nil {
		reportFn = state.metaRaft.ProposeDEKRewrapProgress
	}
	WireDEKPostCommit(fsm, state.metaRaft, isLeaderFn, newRewrapScrubberKick(rewrapCtrl, state.nodeID, reportFn))
	return nil
}

// wireRewrapLanes registers the S6b data-rewrap lanes on the controller created
// in wireDEKKeeper. It MUST run after the backends are built (state.distBackend
// from bootOwnedGroupsAndEC, state.packedBackend from bootBackendWrap) — see
// run.go. A nil controller (encryption not wired) or nil backend is a no-op, so
// it is safe to call unconditionally.
func wireRewrapLanes(state *bootState) {
	if state.rewrapController == nil {
		return
	}
	// EC lane: sweep every data group the node owns. An object physically lives
	// in the placement group recorded in its metadata, which can differ from the
	// bucket's current router assignment after a reassignment — so the lane
	// collects all stored shards (all versions, all buckets) via
	// CollectECRewrapTargets, not a per-bucket router lookup. Ownership uses the
	// stable state.nodeID (the placement vector's identity), not the transport
	// address.
	if state.dgMgr != nil {
		ecLane := cluster.NewECRewrapLane(
			state.nodeID,
			func() []cluster.ECRewrapShardBackend {
				var out []cluster.ECRewrapShardBackend
				for _, dg := range state.dgMgr.All() {
					if gb := dg.Backend(); gb != nil {
						out = append(out, gb)
					}
				}
				return out
			},
		)
		state.rewrapController.RegisterLane(ecLane)
	}
	if state.packedBackend != nil { // single-node packed-blob fast path only
		state.rewrapController.RegisterLane(packblob.NewPackblobRewrapLane(state.packedBackend))
	}

	// FSMValueCheckLane wiring (S7-1a-2).
	//
	// The check-lane gates Kick's completion report until this node's policy:/obj:
	// values are all at keeper-current gen. Registering it here (BEFORE MarkReady)
	// ensures every Kick includes the predicate.
	if state.dgMgr != nil {
		checkLane := cluster.NewFSMValueCheckLane(func() []*cluster.GroupBackend {
			var out []*cluster.GroupBackend
			for _, dg := range state.dgMgr.All() {
				if gb := dg.Backend(); gb != nil {
					out = append(out, gb)
				}
			}
			return out
		})
		state.rewrapController.RegisterLane(checkLane)

	}

	// Signal that all lanes have been registered. Kick refuses (errLanesNotReady)
	// until MarkReady is called, preventing a restart-replay race where Kick fires
	// before all lanes are wired and emits a false completion.
	state.rewrapController.MarkReady()
}

func zeroizeKEKCopy(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
