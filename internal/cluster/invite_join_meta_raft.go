package cluster

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/raft"
)

// errInviteAddressTaken rejects a Phase-2 ACK whose advertised raft address (or
// SPKI) is already owned by a DIFFERENT existing node. Routed to JoinStatusError
// by joinReplyFromError's default branch; the invite stays unconsumed.
var errInviteAddressTaken = errors.New("invite-join: address owned by another node")

// errInviteNodeIDTaken rejects a Phase-2 ACK whose advertised NODE ID is already
// a meta member at a DIFFERENT address. Genesis/KEK members live in MetaFSM.nodes
// but may not be in the zero-CA peer registry, so neither the address-owner nor
// the SPKI-owner guard catches a join that reuses an existing node id with a
// fresh address+SPKI; the dedicated check below does. Routed to JoinStatusError;
// the invite stays unconsumed.
var errInviteNodeIDTaken = errors.New("invite-join: node id owned by another node")

// IsSPKIDenylisted reports whether an SPKI is revoked (invite admission gate).
func (m *MetaRaft) IsSPKIDenylisted(spki [32]byte) bool { return m.fsm.peers.isDenylisted(spki) }

// SPKIOwner returns the node-id that owns an SPKI, if any (invite gate).
func (m *MetaRaft) SPKIOwner(spki [32]byte) (string, bool) { return m.fsm.peers.spkiOwner(spki) }

// LookupInvite returns an invite public key if present, unused, and unexpired.
func (m *MetaRaft) LookupInvite(id string, now time.Time) (ed25519.PublicKey, bool) {
	return m.fsm.invites.lookup(id, now)
}

// AcceptSPKIBytes returns the cluster transport accept-set as raw SPKI bytes.
func (m *MetaRaft) AcceptSPKIBytes() [][]byte { return m.fsm.peers.acceptSPKIBytes() }

// JoinViaInvite stages the MEMBERSHIP for a brand-new node that has already
// passed the invite gate AND been bound to its (nodeID, SPKI) pending-redemption
// record (zero-CA Phase 2, §4.2). It is the Phase-2-ACK membership half of the
// two-phase invite-join handler; the invite-pending binding (Phase-1) and the
// single-use invite consume (Phase-2) are owned by the handler, NOT here.
//
// Idempotency: each step tolerates already-applied state for the SAME
// (nodeID, spki), so a Phase-2 retry after a crash mid-membership resumes from
// the first missing step and completes without error:
//   - ProposeRegisterPendingLearner is already same-(nodeID,spki) idempotent.
//   - AddLearner: ErrAlreadyLearner (already a learner/voter) → continue.
//   - PromoteToVoter: ErrNotALearner (already promoted to voter) → continue.
//   - ProposePromoteMember / ProposeAddNode overwrite FSM state → idempotent.
//
// Rollback policy: a transient AddLearner / promotion failure is NOT the
// joiner's fault, so the handler rolls back with RemoveLearner + dropping the
// pending registry entry instead of denylisting the SPKI (which would
// permanently burn a legitimate identity). Denylisting is reserved for genuine
// revocation (Phase 3). The handler performs the rollback; on failure here we
// return the error so the handler can RemoveLearner.
func (m *MetaRaft) JoinViaInvite(ctx context.Context, nodeID, addr string, spki [32]byte, inviteID string) error {
	raftID := addr
	if raftID == "" {
		raftID = nodeID
	}
	// Ownership guard (BEFORE any membership mutation): raft IDs ARE addresses, so
	// a joiner that advertised an address already owned by a DIFFERENT existing
	// node (e.g. the leader's own raft address — the joiner controls the Phase-1
	// transcript address) would otherwise hit AddLearner's ErrAlreadyLearner /
	// PromoteToVoter's ErrNotALearner, get tolerated as a "resume", and then
	// ProposeAddNode would register a NEW meta member/SPKI over the existing
	// node's address → membership corruption + a consumed single-use invite.
	// Tolerate the already-present states ONLY when the existing entry at that
	// address (and that SPKI) belongs to THIS pending join (same nodeID). Running
	// this before AddLearner also keeps handleJoinPhase2's unconditional
	// RemoveLearner rollback a harmless no-op on rejection.
	if existing, ok := m.fsm.ResolveNodeIDByAddress(addr); ok && existing != nodeID {
		return fmt.Errorf("%w: address %s owned by node %s", errInviteAddressTaken, addr, existing)
	}
	if owner, ok := m.fsm.peers.spkiOwner(spki); ok && owner != nodeID {
		return fmt.Errorf("%w: SPKI owned by node %s", errInviteAddressTaken, owner)
	}
	// Node-id reuse guard: if this nodeID is ALREADY a meta member, a legitimate
	// invite-joiner always has a brand-new generated id, so allow ONLY when the
	// peer registry already binds this exact (nodeID, spki) — proving THIS join's
	// prior attempt registered it (a genuine idempotent retry). Reusing a
	// pre-existing member id (e.g. a genesis/KEK node in f.nodes but NOT the peer
	// registry) with a fresh SPKI is rejected even at the SAME address, preventing
	// an SPKI takeover that the address/SPKI-owner guards above would miss. Running
	// this BEFORE any membership mutation leaves the invite unconsumed on reject.
	if _, ok := m.fsm.NodeByID(nodeID); ok {
		if owner, ok := m.fsm.peers.spkiOwner(spki); !ok || owner != nodeID {
			return fmt.Errorf("%w: node %s already a member", errInviteNodeIDTaken, nodeID)
		}
	}
	// commit1: register pending-learner SPKI + AddLearner.
	if err := m.ProposeRegisterPendingLearner(ctx, nodeID, spki, addr); err != nil {
		return err
	}
	if err := m.node.AddLearner(raftID, addr); err != nil && !errors.Is(err, raft.ErrAlreadyLearner) {
		return fmt.Errorf("meta_raft: AddLearner %s: %w", nodeID, err)
	}
	// commit2: promote. ErrNotALearner means the node was already promoted to a
	// voter on a prior attempt — treat as done and continue.
	if err := m.node.PromoteToVoter(raftID); err != nil && !errors.Is(err, raft.ErrNotALearner) {
		return fmt.Errorf("meta_raft: PromoteToVoter %s: %w", nodeID, err)
	}
	if err := m.ProposePromoteMember(ctx, nodeID); err != nil {
		return err
	}
	return m.ProposeAddNode(ctx, MetaNodeEntry{ID: nodeID, Address: addr, Role: 0})
}

// LookupPending returns the (nodeID, spki, addr) bound to a Phase-1
// pending-redemption invite record; ok is false if the invite has no pending
// binding. Used by the two-phase join handler to validate a Phase-2 ACK.
func (m *MetaRaft) LookupPending(inviteID string) (nodeID string, spki [32]byte, addr string, ok bool) {
	return m.fsm.invites.lookupPending(inviteID)
}

// RemoveLearner drops an un-promoted learner from the raft membership. Used by
// the two-phase join handler to roll back a Phase-2 membership attempt that
// failed before promotion completed.
func (m *MetaRaft) RemoveLearner(nodeID, addr string) error {
	raftID := addr
	if raftID == "" {
		raftID = nodeID
	}
	return m.node.RemoveLearner(raftID)
}
