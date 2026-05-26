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
	r.byNodeID[nodeID] = peerEntry{NodeID: nodeID, SPKI: s, Address: addr, State: peerStatePendingLearner}
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

func (r *peerRegistry) lookupByNodeID(nodeID string) (peerEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.byNodeID[nodeID]
	return e, ok
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
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([][]byte, 0, len(r.byNodeID))
	for _, e := range r.byNodeID {
		s := e.SPKI
		out = append(out, append([]byte(nil), s[:]...))
	}
	return out
}
