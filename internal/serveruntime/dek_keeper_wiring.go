package serveruntime

import (
	"errors"
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/nodeconfig"
)

// wireDEKKeeper reads the node KEK source from nodeconfig, loads (or generates)
// the 32-byte KEK, constructs the cluster DEK Keeper + the shared
// HandshakeVerifier, injects the keeper into the FSM, and registers the DEK
// post-commit hook.
//
// KEK load mode (§7 B3 / F#21):
//
//   - First-cluster-init mode (NOT joining, NO peers): LoadOrGenerateKEK is
//     correct — this is the very first node bootstrapping its own KEK.
//
//   - Join mode (`--join-pending` OR peers configured): strict LoadKEK. If
//     kek.key is missing, return a remediation error rather than silently
//     auto-generating a fresh KEK that would never decrypt the cluster's
//     wrapped DEKs. wireDEKKeeper runs BEFORE Restore, so the post-Restore
//     guard in rebuildDEKKeeperFromRestore (which only fires when the
//     snapshot trailer carries DEKs) cannot catch first-time joiners.
//
// Extracted from bootMetaRaftWiring so the wiring path is directly testable
// without standing up a full raft+badger fixture. LoadOrGenerateKEK enforces
// the file:// scheme and returns ErrUnsupportedKEKSource for kms:// (deferred
// to Phase 3) — the error path here surfaces that to the operator.
//
// proposer and scrubberKick are passed nil; the §6 audit lane will replace
// them when the rewrap scrubber lands. WireDEKPostCommit's dispatch handles
// both nils safely (DEKRotate still mints a new generation; the scrubber and
// version-prune propagation just don't fire).
func wireDEKKeeper(state *bootState, fsm *cluster.MetaFSM) error {
	dekSrc := nodeconfig.New(state.cfg.DataDir).KEKSource()
	kek, err := loadKEKForBoot(state, dekSrc)
	if err != nil {
		return err
	}
	keeper, err := encrypt.NewDEKKeeper(kek)
	if err != nil {
		return fmt.Errorf("init DEK keeper: %w", err)
	}
	fsm.SetDEKKeeper(keeper)
	state.dekKeeper = keeper
	state.kek = kek
	state.handshakeVerifier = encrypt.NewHandshakeVerifier(kek)
	WireDEKPostCommit(fsm, nil /* proposer (§6) */, keeper, nil /* scrubberKick (§6) */)
	return nil
}

// loadKEKForBoot dispatches KEK loading by boot mode. See wireDEKKeeper.
func loadKEKForBoot(state *bootState, dekSrc string) ([]byte, error) {
	if state.joinMode || len(state.peers) > 0 {
		kek, err := encrypt.LoadKEK(dekSrc)
		if err != nil {
			if errors.Is(err, encrypt.ErrKEKNotFound) {
				return nil, fmt.Errorf("KEK not found at %s and this node is configured to join an existing cluster. "+
					"A joining node MUST already hold the cluster's KEK before serve can start — "+
					"auto-generating a fresh KEK here would produce a node whose KEK never decrypts "+
					"the cluster's FSM-wrapped DEKs. Options: "+
					"(a) restore kek.key from backup or scp from any healthy peer "+
					"(the KEK is identical on every cluster node), "+
					"(b) use `grainfs cluster join <peer>` from the joining node to complete the "+
					"offline-bootstrap handshake (still requires the KEK in place locally), "+
					"(c) decommission this node and rejoin from scratch (loses local raft state). "+
					"Underlying error: %w", dekSrc, err)
			}
			return nil, fmt.Errorf("load KEK from %s: %w", dekSrc, err)
		}
		return kek, nil
	}
	kek, err := encrypt.LoadOrGenerateKEK(dekSrc)
	if err != nil {
		return nil, fmt.Errorf("load KEK from %s: %w", dekSrc, err)
	}
	return kek, nil
}
