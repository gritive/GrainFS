package serveruntime

import (
	"errors"
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/nodeconfig"
)

// bootDEKKeeperFromRestore runs AFTER bootMetaRaftStart, by which time raft
// Restore (if any) has populated MetaFSM.PendingDEKVersions. When the snapshot
// trailer carries wrapped DEKs, the fresh keeper installed by wireDEKKeeper
// (NewDEKKeeper) is replaced with one reconstructed from those wrapped bytes
// via encrypt.LoadFromFSM.
//
// This is the operator-visible failure point for the two §7 F#21 / F#22 modes:
//
//   - KEK missing (F#21): the raft snapshot has wrapped DEKs but the node's
//     kek.key file is gone. LoadKEK returns ErrKEKNotFound; we surface a
//     3-option remediation message (restore from backup, scp from a healthy
//     peer, or decommission and rejoin).
//
//   - KEK wrong (F#22): kek.key exists but does not decrypt the wrapped DEKs
//     (typically because the operator rotated/replaced it). LoadFromFSM
//     returns an unwrap error; we wrap it with a remediation pointing at
//     scp-from-peer.
//
// When the snapshot has NO wrapped DEKs (PendingDEKVersions returns nil or
// empty), this phase is a no-op — the fresh keeper from wireDEKKeeper is left
// in place. That covers the first-boot path where wireDEKKeeper's
// LoadOrGenerateKEK created kek.key from scratch.
func bootDEKKeeperFromRestore(state *bootState, fsm *cluster.MetaFSM) error {
	versions, _ := fsm.PendingDEKVersions()
	if len(versions) == 0 {
		// No DKVS trailer in the snapshot — first boot, or a snapshot taken
		// before any DEK was rotated. wireDEKKeeper's fresh keeper is correct.
		return nil
	}

	src := nodeconfig.New(state.cfg.DataDir).KEKSource()
	kek, err := encrypt.LoadKEK(src)
	if err != nil {
		if errors.Is(err, encrypt.ErrKEKNotFound) {
			return fmt.Errorf("KEK not found at %s and the raft snapshot carries FSM-wrapped DEKs that require it. "+
				"This node cannot decrypt cluster state. Options: "+
				"(a) restore kek.key from backup or scp from any healthy peer "+
				"(the KEK is identical on every cluster node), "+
				"(b) decommission this node and rejoin from scratch (loses local raft state). "+
				"Underlying error: %w", src, err)
		}
		return fmt.Errorf("load KEK from %s: %w", src, err)
	}

	keeper, err := encrypt.LoadFromFSM(kek, versions)
	if err != nil {
		return fmt.Errorf("decrypt FSM-wrapped DEK with KEK at %s: %w. "+
			"This usually means the KEK on this node was rotated or replaced "+
			"and no longer matches the cluster's wrapped DEKs. "+
			"Restore the matching KEK by scp from any healthy peer.", src, err)
	}

	fsm.SetDEKKeeper(keeper)
	state.dekKeeper = keeper
	return nil
}
