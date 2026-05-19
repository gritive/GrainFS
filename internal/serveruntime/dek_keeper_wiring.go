package serveruntime

import (
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/nodeconfig"
)

// wireDEKKeeper reads the node KEK source from nodeconfig, loads (or generates)
// the 32-byte KEK, constructs the cluster DEK Keeper, injects it into the FSM,
// and registers the DEK post-commit hook.
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
	kek, err := encrypt.LoadOrGenerateKEK(dekSrc)
	if err != nil {
		return fmt.Errorf("load KEK from %s: %w", dekSrc, err)
	}
	keeper, err := encrypt.NewDEKKeeper(kek)
	if err != nil {
		return fmt.Errorf("init DEK keeper: %w", err)
	}
	fsm.SetDEKKeeper(keeper)
	state.dekKeeper = keeper
	WireDEKPostCommit(fsm, nil /* proposer (§6) */, keeper, nil /* scrubberKick (§6) */)
	return nil
}
