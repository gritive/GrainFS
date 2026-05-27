package cluster

import (
	"errors"
	"fmt"
	"sync"
)

type peerState int

const (
	peerStatePendingLearner peerState = iota
	peerStateMember
)

type peerEntry struct {
	NodeID  string
	SPKI    [32]byte
	Address string
	State   peerState
	// PresentsPerNode records whether this peer presents a per-node identity
	// (D-rev3). Recording-only plumbing; Task 7 reads it.
	PresentsPerNode bool
}

// peerRegistry is the deterministic membership/SPKI registry applied from the
// Raft log. SPKI → node-id is injective so per-peer dial pinning is unambiguous.
// Learners ARE transport-accepted (so they can catch up) but non-voting until
// promoteMember.
type peerRegistry struct {
	mu       sync.RWMutex
	byNodeID map[string]peerEntry
	bySPKI   map[[32]byte]string
	deny     map[[32]byte]struct{}
}

func newPeerRegistry() *peerRegistry {
	return &peerRegistry{
		byNodeID: make(map[string]peerEntry),
		bySPKI:   make(map[[32]byte]string),
		deny:     make(map[[32]byte]struct{}),
	}
}

var (
	errSPKINotUnique  = errors.New("SPKI already registered under another node-id")
	errSPKIDenylisted = errors.New("SPKI is denylisted")
	errNodeIDRebind   = errors.New("node-id already registered with a different SPKI")
)

func (r *peerRegistry) registerPendingLearner(nodeID string, s [32]byte, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, denied := r.deny[s]; denied {
		return errSPKIDenylisted
	}
	if owner, ok := r.bySPKI[s]; ok && owner != nodeID {
		return fmt.Errorf("%w: owned by %s", errSPKINotUnique, owner)
	}
	// node-id uniqueness: an invite admits exactly one NEW identity. A node-id
	// already bound to a DIFFERENT SPKI must not be silently rebound — otherwise
	// a leaked single-use invite could hijack an existing member's node-id with
	// an attacker-owned SPKI.
	if existing, ok := r.byNodeID[nodeID]; ok && existing.SPKI != s {
		return fmt.Errorf("%w: node %s already bound to a different SPKI", errNodeIDRebind, nodeID)
	}
	r.byNodeID[nodeID] = peerEntry{NodeID: nodeID, SPKI: s, Address: addr, State: peerStatePendingLearner}
	r.bySPKI[s] = nodeID
	return nil
}

// registerMember is non-demoting boot-time self-registration (D-rev3 step 2).
// If the node is absent it is inserted directly as a member. If it is already
// present with the SAME (nodeID, SPKI) it is kept/UPGRADED to member — an
// existing member stays member, a pending-learner is promoted; it is NEVER
// downgraded. Address and PresentsPerNode are refreshed last-write-wins on an
// idempotent re-register. The same SPKI-uniqueness and node-id-rebind guards as
// registerPendingLearner apply. presentsPerNode is recording-only (Task 7).
func (r *peerRegistry) registerMember(nodeID string, s [32]byte, addr string, presentsPerNode bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, denied := r.deny[s]; denied {
		return errSPKIDenylisted
	}
	if owner, ok := r.bySPKI[s]; ok && owner != nodeID {
		return fmt.Errorf("%w: owned by %s", errSPKINotUnique, owner)
	}
	if existing, ok := r.byNodeID[nodeID]; ok && existing.SPKI != s {
		return fmt.Errorf("%w: node %s already bound to a different SPKI", errNodeIDRebind, nodeID)
	}
	r.byNodeID[nodeID] = peerEntry{NodeID: nodeID, SPKI: s, Address: addr, State: peerStateMember, PresentsPerNode: presentsPerNode}
	r.bySPKI[s] = nodeID
	return nil
}

func (r *peerRegistry) promoteMember(nodeID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.byNodeID[nodeID]
	if !ok {
		return fmt.Errorf("promote: unknown node %s", nodeID)
	}
	e.State = peerStateMember
	r.byNodeID[nodeID] = e
	return nil
}

// export returns a snapshot copy of every registered entry (member AND
// pending-learner). Used by MetaFSM.Snapshot to serialize the registry into the
// meta-state snapshot so the per-node accept-set survives log compaction.
func (r *peerRegistry) export() []peerEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]peerEntry, 0, len(r.byNodeID))
	for _, e := range r.byNodeID {
		out = append(out, e)
	}
	return out
}

// importEntries rebuilds the registry from a snapshot's entries. It REPLACES
// the byNodeID + bySPKI indexes wholesale (state restore — NOT a register*
// flow). The deny set is left untouched (it is not part of the snapshot; see
// Task 5 report). State and PresentsPerNode are preserved verbatim.
//
// Validation: a corrupt meta snapshot is fatal, so importEntries hard-errors on
// any malformed/inconsistent entry BEFORE mutating the live indexes (build into
// locals, commit only if every entry passes — otherwise the registry would be
// left half-cleared). Checks: non-empty node ID; valid State enum; SPKI not
// zero (a short SPKI copy zero-pads, so zero is the corruption signal — the
// decode loop in meta_fsm_snapshot.go also rejects wrong-length SPKI upstream);
// no duplicate node ID; no duplicate SPKI (so bySPKI can never point at two
// node-ids). Deterministic across nodes: same bytes → same error.
func (r *peerRegistry) importEntries(entries []peerEntry) error {
	byNodeID := make(map[string]peerEntry, len(entries))
	bySPKI := make(map[[32]byte]string, len(entries))
	for i, e := range entries {
		if e.NodeID == "" {
			return fmt.Errorf("importEntries: entry[%d] has empty node ID", i)
		}
		if e.State != peerStatePendingLearner && e.State != peerStateMember {
			return fmt.Errorf("importEntries: node %s has invalid state %d", e.NodeID, e.State)
		}
		if e.SPKI == ([32]byte{}) {
			return fmt.Errorf("importEntries: node %s has zero/malformed SPKI", e.NodeID)
		}
		if _, dup := byNodeID[e.NodeID]; dup {
			return fmt.Errorf("importEntries: duplicate node ID %s", e.NodeID)
		}
		if owner, dup := bySPKI[e.SPKI]; dup {
			return fmt.Errorf("importEntries: duplicate SPKI shared by node %s and node %s", owner, e.NodeID)
		}
		byNodeID[e.NodeID] = e
		bySPKI[e.SPKI] = e.NodeID
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byNodeID = byNodeID
	r.bySPKI = bySPKI
	return nil
}

// spkiOwner returns the node-id that owns an SPKI (Task 5 uses this).
func (r *peerRegistry) spkiOwner(s [32]byte) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	owner, ok := r.bySPKI[s]
	return owner, ok
}

func (r *peerRegistry) isDenylisted(s [32]byte) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.deny[s]
	return ok
}

func (r *peerRegistry) denylist(s [32]byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deny[s] = struct{}{}
}

// remove deletes a node's entry (Phase 3 revoke). Returns the removed SPKI.
func (r *peerRegistry) remove(nodeID string) ([32]byte, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.byNodeID[nodeID]
	if !ok {
		return [32]byte{}, false
	}
	delete(r.byNodeID, nodeID)
	delete(r.bySPKI, e.SPKI)
	return e.SPKI, true
}

// acceptSPKIs returns every registered SPKI (pending-learner AND member) — the
// set the transport accepts.
func (r *peerRegistry) acceptSPKIs() [][32]byte {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([][32]byte, 0, len(r.byNodeID))
	for _, e := range r.byNodeID {
		out = append(out, e.SPKI)
	}
	return out
}

// acceptSPKIBytes is acceptSPKIs() as [][]byte (Task 5 JoinReply.PeerSPKIs).
func (r *peerRegistry) acceptSPKIBytes() [][]byte {
	spkis := r.acceptSPKIs()
	out := make([][]byte, len(spkis))
	for i, s := range spkis {
		out[i] = append([]byte(nil), s[:]...)
	}
	return out
}
