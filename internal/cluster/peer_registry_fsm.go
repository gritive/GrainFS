package cluster

import (
	"bytes"
	"fmt"
	"sort"
)

// Peers returns the peer registry sub-FSM.
func (f *MetaFSM) Peers() *peerRegistry { return f.peers }

// SetOnPeersChanged wires a side-effect callback fired after each peer-registry
// command commits. The callback receives the new transport accept-set and is
// expected to rebuild the QUIC listener's accepted-SPKI window. Called from the
// FSM apply goroutine; set before MetaRaft.Start().
//
// TODO(phase-2-followup): membership vs rotation accept-set union. A concurrent
// KEK rotation also drives SwapIdentity with a 2-SPKI window ([OldSPKI,NewSPKI]);
// a membership change during a rotation would overwrite that window with the
// registry accept set. Union-of-both is out of Phase 2 scope.
func (f *MetaFSM) SetOnPeersChanged(fn func([][32]byte)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onPeersChanged = fn
}

// SetOnClusterKeyDropped wires the side-effect callback fired once when Restore
// decodes a snapshot whose cluster_key_dropped bit is true (spec §8 H3). The
// callback drops the transport's cluster-key base on this node. Called from the
// FSM apply goroutine; set before MetaRaft.Start(). Dormant in PR-1 (no snapshot
// carries true).
func (f *MetaFSM) SetOnClusterKeyDropped(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onClusterKeyDropped = fn
}

// SetOnPresentFlip wires the side-effect callback fired on the first
// false→true BeginPresentFlip Apply, and on Restore of a snapshot whose
// present_flip_begun bit is true (PR-2a §8c). The callback instructs the
// transport to PRESENT the per-node cert. Set before MetaRaft.Start().
func (f *MetaFSM) SetOnPresentFlip(fn func()) {
	f.mu.Lock()
	f.onPresentFlip = fn
	f.mu.Unlock()
}

// PresentFlipBegun reports whether the present-flip has been committed in the
// raft log (i.e., BeginPresentFlip has been applied). Safe for concurrent use.
func (f *MetaFSM) PresentFlipBegun() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.presentFlipBegun
}

// SetPresentFlipBegunForTest directly sets the presentFlipBegun field.
// Test-only: used to seed the field before Snapshot() in round-trip tests.
func (f *MetaFSM) SetPresentFlipBegunForTest(v bool) {
	f.mu.Lock()
	f.presentFlipBegun = v
	f.mu.Unlock()
}

// PeerSPKIs returns a sorted snapshot of all registered peer SPKIs. Safe for
// concurrent use. Used by the invite-join Phase-1 leader to populate
// peer_spkis in the sealed bootstrap (PR-2a §8f M1).
func (f *MetaFSM) PeerSPKIs() [][32]byte {
	out := f.peers.acceptSPKIs()
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i][:], out[j][:]) < 0
	})
	return out
}

// PeerNodeIDToSPKI returns a snapshot of the nodeID→SPKI map for all
// registered peers. Safe for concurrent use.
func (f *MetaFSM) PeerNodeIDToSPKI() map[string][32]byte {
	return f.peers.nodeIDToSPKI()
}

// ClusterKeyDropped reports whether the cluster-key-drop has been committed
// in the raft log. Safe for concurrent use.
func (f *MetaFSM) ClusterKeyDropped() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.clusterKeyDropped
}

// firePeersChanged snapshots the callback under lock then invokes it with the
// current accept-set OUTSIDE the lock (the callback mutates transport state).
//
// TODO(phase-2-followup): membership vs rotation accept-set union. The accept
// set passed here overwrites any in-flight rotation 2-SPKI window; union-of-both
// is out of Phase 2 scope.
func (f *MetaFSM) firePeersChanged() {
	f.mu.RLock()
	cb := f.onPeersChanged
	f.mu.RUnlock()
	if cb == nil {
		return
	}
	cb(f.peers.acceptSPKIs())
}

// applyRegisterPendingLearner decodes a MetaRegisterPendingLearnerCmd and
// registers the peer; on success it rebuilds the transport accept-set.
func (f *MetaFSM) applyRegisterPendingLearner(data []byte) error {
	nodeID, spki, addr, err := decodeRegisterPendingLearnerCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: decode RegisterPendingLearner: %w", err)
	}
	if err := f.peers.registerPendingLearner(nodeID, spki, addr); err != nil {
		return fmt.Errorf("meta_fsm: RegisterPendingLearner %q: %w", nodeID, err)
	}
	f.firePeersChanged()
	return nil
}

// applyRegisterMember decodes a MetaRegisterMemberCmd and registers the peer as
// a member without ever demoting an existing entry; on success it rebuilds the
// transport accept-set.
func (f *MetaFSM) applyRegisterMember(data []byte) error {
	nodeID, spki, addr, presentsPerNode, err := decodeRegisterMemberCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: decode RegisterMember: %w", err)
	}
	if err := f.peers.registerMember(nodeID, spki, addr, presentsPerNode); err != nil {
		return fmt.Errorf("meta_fsm: RegisterMember %q: %w", nodeID, err)
	}
	f.firePeersChanged()
	return nil
}

// applyPromoteMember decodes a MetaPromoteMemberCmd and promotes the peer.
func (f *MetaFSM) applyPromoteMember(data []byte) error {
	nodeID, err := decodePromoteMemberCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: decode PromoteMember: %w", err)
	}
	if err := f.peers.promoteMember(nodeID); err != nil {
		return fmt.Errorf("meta_fsm: PromoteMember %q: %w", nodeID, err)
	}
	f.firePeersChanged()
	return nil
}

// applyRevokePeer decodes a MetaRevokePeerCmd, removes the peer, and denylists
// its SPKI so a revoked identity can't re-register.
func (f *MetaFSM) applyRevokePeer(data []byte) error {
	nodeID, err := decodeRevokePeerCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: decode RevokePeer: %w", err)
	}
	if spki, ok := f.peers.remove(nodeID); ok {
		f.peers.denylist(spki)
	}
	f.firePeersChanged()
	return nil
}
