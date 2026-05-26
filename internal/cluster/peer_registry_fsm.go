package cluster

import "fmt"

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
