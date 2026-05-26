package cluster

import "fmt"

// SetFrozenSegmentPathSource injects the snapshot Manager's AllFrozenSegmentPaths
// so this backend can supply the snapshot half of the scrubber's known-set. Call
// once during boot, before the scrubber starts.
func (b *DistributedBackend) SetFrozenSegmentPathSource(fn func() (map[string][]string, error)) {
	b.frozenSegSrc = fn
}

// AllFrozenSegmentPaths returns snapshot-frozen segment paths grouped by bucket.
// Fails closed when no source is wired: the scrubber treats the error as "skip
// the segment sweep this cycle", so an un-wired backend never sweeps against an
// incomplete known-set.
func (b *DistributedBackend) AllFrozenSegmentPaths() (map[string][]string, error) {
	if b.frozenSegSrc == nil {
		return nil, fmt.Errorf("frozen segment path source not wired")
	}
	return b.frozenSegSrc()
}

// CaughtUp reports whether this node's applied FSM state is provably current,
// gating the scrubber's orphan-segment GC against data loss.
//
// The scrubber builds its known-set from ListAllObjects(), a stale local Badger
// read with no linearizable barrier. On a cluster follower whose Raft apply
// lags, the local FSM may not yet hold a committed object's metadata, so a
// committed segment could look orphaned and be wrongly deleted. CaughtUp blocks
// GC until applied >= the node's locally-known commit index.
//
// Conservative by design: a false negative merely delays GC one cycle (safe);
// a false positive risks data loss. Single-node / unit-test path (no raft node)
// is trivially current and returns true so GC can run.
//
// Limitation: CommittedIndex() is the node's LOCAL view. A follower whose latest
// AppendEntries has not yet landed has a stale local commit, so applied >=
// localCommit can hold while the cluster has committed further. This matches the
// scrubber's own semantics — its known-set is also a local read — so the gate is
// consistent with what it protects, not a global linearizability guarantee.
func (b *DistributedBackend) CaughtUp() bool {
	if b.node == nil {
		return true
	}
	return b.lastApplied.Load() >= b.node.CommittedIndex()
}
