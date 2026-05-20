package serveruntime

import (
	"errors"
	"fmt"

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
//     the keeper (DEKRotate, DEKVersionPrune, JWTSigningKeyRotate). Calling
//     SetDEKKeeper after `go runApplyLoop` would race with a queued rotation
//     that mutates the fresh (wrong) keeper, causing silent data loss when
//     the post-Restore swap then overwrites that rotation.
//
// When the snapshot trailer carries wrapped DEKs, the fresh keeper installed
// by wireDEKKeeper (NewDEKKeeper) is replaced with one reconstructed via
// encrypt.LoadFromFSM. This is the operator-visible failure point for the
// two §7 F#21 / F#22 modes:
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
// When the snapshot has NO wrapped DEKs (PendingDEKVersions returns empty),
// this is a no-op — the fresh keeper from wireDEKKeeper is left in place.
// That covers the first-boot path where wireDEKKeeper's LoadOrGenerateKEK
// created kek.key from scratch.
func rebuildDEKKeeperFromRestore(state *bootState, fsm *cluster.MetaFSM) error {
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
