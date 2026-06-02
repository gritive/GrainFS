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
	// NodeKeyKEKGen records the KEK generation sealing keys.d/node.key.enc.
	// KEK prune refuses while a voter reports a generation at or below the
	// prune target.
	NodeKeyKEKGen uint32
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
// downgraded. Address is refreshed last-write-wins on an idempotent
// re-register; PresentsPerNode is monotone (never true->false) and
// NodeKeyKEKGen is monotone (never regresses to an older sealing generation).
// The same SPKI-uniqueness and node-id-rebind guards as registerPendingLearner
// apply.
func (r *peerRegistry) registerMember(nodeID string, s [32]byte, addr string, presentsPerNode bool, nodeKeyKEKGen uint32) error {
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
	// presents_per_node is MONOTONE (spec §8 H6): once true it never regresses
	// via RegisterMember (a restart self-registers false and must not un-flip the
	// gate bit). Resets only on explicit member removal (revoke slice).
	if existing, ok := r.byNodeID[nodeID]; ok && existing.PresentsPerNode {
		presentsPerNode = true
	}
	if existing, ok := r.byNodeID[nodeID]; ok && existing.NodeKeyKEKGen > nodeKeyKEKGen {
		nodeKeyKEKGen = existing.NodeKeyKEKGen
	}
	r.byNodeID[nodeID] = peerEntry{NodeID: nodeID, SPKI: s, Address: addr, State: peerStateMember, PresentsPerNode: presentsPerNode, NodeKeyKEKGen: nodeKeyKEKGen}
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

// validatePeerEntries validates a snapshot's peer entries and BUILDS the
// byNodeID/bySPKI indexes WITHOUT touching the live registry. It REPLACES the
// indexes wholesale on commit (state restore — NOT a register* flow). State and
// PresentsPerNode are preserved verbatim.
//
// Validation: a corrupt meta snapshot is fatal, so this hard-errors on any
// malformed/inconsistent entry. Checks: non-empty node ID; valid State enum;
// SPKI not zero (a short SPKI copy zero-pads, so zero is the corruption signal —
// the decode loop in meta_fsm_snapshot.go also rejects wrong-length SPKI
// upstream); no duplicate node ID; no duplicate SPKI (so bySPKI can never point
// at two node-ids). Deterministic across nodes: same bytes → same error.
//
// The meta-FSM Restore decode phase calls this EARLY so a corrupt peer vector
// returns an error BEFORE any core FSM state is committed (no partial restore);
// the commit phase then calls commitPeerIndexes with the pre-validated maps,
// which cannot fail.
func validatePeerEntries(entries []peerEntry) (map[string]peerEntry, map[[32]byte]string, error) {
	byNodeID := make(map[string]peerEntry, len(entries))
	bySPKI := make(map[[32]byte]string, len(entries))
	for i, e := range entries {
		if e.NodeID == "" {
			return nil, nil, fmt.Errorf("importEntries: entry[%d] has empty node ID", i)
		}
		if e.State != peerStatePendingLearner && e.State != peerStateMember {
			return nil, nil, fmt.Errorf("importEntries: node %s has invalid state %d", e.NodeID, e.State)
		}
		if e.SPKI == ([32]byte{}) {
			return nil, nil, fmt.Errorf("importEntries: node %s has zero/malformed SPKI", e.NodeID)
		}
		if _, dup := byNodeID[e.NodeID]; dup {
			return nil, nil, fmt.Errorf("importEntries: duplicate node ID %s", e.NodeID)
		}
		if owner, dup := bySPKI[e.SPKI]; dup {
			return nil, nil, fmt.Errorf("importEntries: duplicate SPKI shared by node %s and node %s", owner, e.NodeID)
		}
		byNodeID[e.NodeID] = e
		bySPKI[e.SPKI] = e.NodeID
	}
	return byNodeID, bySPKI, nil
}

// commitPeerIndexes swaps pre-validated indexes (from validatePeerEntries) into
// the live registry under lock. It is the unfailable commit half of the
// validate/commit split — Restore calls it only after every other section has
// been committed.
func (r *peerRegistry) commitPeerIndexes(byNodeID map[string]peerEntry, bySPKI map[[32]byte]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byNodeID = byNodeID
	r.bySPKI = bySPKI
}

func (r *peerRegistry) exportDenylist() [][32]byte {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([][32]byte, 0, len(r.deny))
	for s := range r.deny {
		out = append(out, s)
	}
	return out
}

func validateDenylistEntries(entries [][32]byte) (map[[32]byte]struct{}, error) {
	deny := make(map[[32]byte]struct{}, len(entries))
	for i, s := range entries {
		if s == ([32]byte{}) {
			return nil, fmt.Errorf("importDenylist: entry[%d] has zero/malformed SPKI", i)
		}
		if _, dup := deny[s]; dup {
			return nil, fmt.Errorf("importDenylist: duplicate SPKI at entry[%d]", i)
		}
		deny[s] = struct{}{}
	}
	return deny, nil
}

func (r *peerRegistry) commitDenylist(deny map[[32]byte]struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deny = deny
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

// remove deletes a node's entry (Phase 3 revoke). Returns the removed entry.
func (r *peerRegistry) remove(nodeID string) (peerEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.byNodeID[nodeID]
	if !ok {
		return peerEntry{}, false
	}
	delete(r.byNodeID, nodeID)
	delete(r.bySPKI, e.SPKI)
	return e, true
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

// allVotersPresentsPerNode reports whether every voter in the given list has a
// registered entry with PresentsPerNode=true. Voters are raft addresses, the
// same key space as raftAddrToSPKI. Missing voters or false readiness fail the
// D-cut4 gate before DropClusterKeyAccept.
func (r *peerRegistry) allVotersPresentsPerNode(voters []string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, addr := range voters {
		found := false
		for _, e := range r.byNodeID {
			if e.Address != addr {
				continue
			}
			if !e.PresentsPerNode {
				return false
			}
			found = true
			break
		}
		if !found {
			return false
		}
	}
	return true
}

// validateVoterNodeKeyKEKGens verifies every current voter has peer-registry
// evidence proving its local node.key.enc is sealed under a generation that
// survives pruning pruneVersion. Voters are raft addresses.
func (r *peerRegistry) validateVoterNodeKeyKEKGens(voters []string, pruneVersion uint32) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, addr := range voters {
		found := false
		for _, e := range r.byNodeID {
			if e.Address != addr {
				continue
			}
			found = true
			if e.NodeKeyKEKGen <= pruneVersion {
				return fmt.Errorf("voter %s node_key_kek_gen=%d <= prune version %d", addr, e.NodeKeyKEKGen, pruneVersion)
			}
			break
		}
		if !found {
			return fmt.Errorf("missing node-key KEK generation evidence from voter %s", addr)
		}
	}
	return nil
}

// nodeIDToSPKI returns a snapshot of the nodeID→SPKI mapping for all peers.
func (r *peerRegistry) nodeIDToSPKI() map[string][32]byte {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string][32]byte, len(r.byNodeID))
	for id, e := range r.byNodeID {
		out[id] = e.SPKI
	}
	return out
}

// raftAddrToSPKI returns a snapshot of the raftAddr→SPKI mapping for all peers.
// Production raft server IDs are transport addresses (MetaRaftConfig.RaftID), so
// callers that receive a voter list from EffectiveConfiguration must use this
// map (not nodeIDToSPKI) to cross-reference voters with their SPKIs.
func (r *peerRegistry) raftAddrToSPKI() map[string][32]byte {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string][32]byte, len(r.byNodeID))
	for _, e := range r.byNodeID {
		if e.Address != "" {
			out[e.Address] = e.SPKI
		}
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
