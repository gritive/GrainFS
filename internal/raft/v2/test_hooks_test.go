package raftv2

// Test-only hooks for the actor state. These methods route through the
// actor goroutine so callers never race with the actor's own writes to
// matchIndex / nextIndex etc.

// peerMatchIndexForTest returns the actor's tracked matchIndex[peer].
// Test-only — production callers must use the public API.
func (n *Node) peerMatchIndexForTest(peer string) uint64 {
	reply := make(chan uint64, 1)
	select {
	case n.cmdCh <- command{kind: cmdTestPeekMatchIndex, learnerID: peer, testMatchReply: reply}:
	case <-n.stopCh:
		return 0
	}
	select {
	case v := <-reply:
		return v
	case <-n.stopCh:
		return 0
	}
}

// setPeerMatchIndexForTest forces the actor's matchIndex[peer] to v.
// Test-only — used to exercise the PromoteToVoter catchup gate without
// having to actually stall replication.
func (n *Node) setPeerMatchIndexForTest(peer string, v uint64) {
	done := make(chan struct{})
	select {
	case n.cmdCh <- command{kind: cmdTestSetMatchIndex, learnerID: peer, testMatchValue: v, testDone: done}:
	case <-n.stopCh:
		return
	}
	select {
	case <-done:
	case <-n.stopCh:
	}
}
