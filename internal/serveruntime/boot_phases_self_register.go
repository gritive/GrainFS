package serveruntime

import (
	"context"
	"encoding/hex"

	"github.com/rs/zerolog/log"
)

// memberSelfRegistrar is the consumer-side slice of *cluster.MetaRaft that
// boot-time self-registration depends on. Defined here (at the call site) per
// the repo's "interfaces at the consumer" rule so the boot step is testable
// with a stub. *cluster.MetaRaft satisfies it.
type memberSelfRegistrar interface {
	ProposeRegisterMember(ctx context.Context, nodeID string, spki [32]byte, addr string, presentsPerNode bool) error
}

// selfRegisterMember proposes this node's OWN per-node SPKI into the peer
// registry (Zero-CA spec §6 D-rev3 step 2). PSK-bridged: the PSK SPKI is still
// in the accept-set, so a booting node is accepted via PSK and can reach the
// leader to submit this proposal. presentsPerNode is always false — the
// foundation never flips the presented cert (Task 7 plumbing).
//
// Idempotent across restarts: Task 4's registerMember is non-demoting, so a
// re-propose with identical args is a no-op-equivalent. The boot step itself
// does not dedup; the FSM does.
//
// Skips cleanly when the registrar is nil (single non-cluster configs that
// never build a meta-raft) or the SPKI is zero (encryption-less test configs
// that never persist a per-node transport identity).
func selfRegisterMember(ctx context.Context, mr memberSelfRegistrar, nodeID string, spki [32]byte, addr string) error {
	if mr == nil || spki == ([32]byte{}) {
		return nil
	}
	if err := mr.ProposeRegisterMember(ctx, nodeID, spki, addr, false); err != nil {
		return err
	}
	log.Info().
		Str("node_id", nodeID).
		Str("spki_prefix", hex.EncodeToString(spki[:8])).
		Str("addr", addr).
		Msg("zero-ca: self-registered per-node SPKI into peer registry (§6 D-rev3 step2)")
	return nil
}

// bootSelfRegisterMember is the boot-phase wrapper: it pulls the node identity
// fields from bootState and delegates to selfRegisterMember.
//
// Ordering: MUST run AFTER bootWALAndForwarders (which installs the meta-raft
// forwarder so a follower's Propose reaches the leader) AND after invite-join
// Phase-2 membership promotion (also inside bootWALAndForwarders). Wired last
// in run.go's post-join sequence, after bootNodeServices.
func bootSelfRegisterMember(ctx context.Context, state *bootState) error {
	if state.metaRaft == nil {
		return selfRegisterMember(ctx, nil, state.nodeID, state.perNodeSPKI, state.raftAddr)
	}
	return selfRegisterMember(ctx, state.metaRaft, state.nodeID, state.perNodeSPKI, state.raftAddr)
}
