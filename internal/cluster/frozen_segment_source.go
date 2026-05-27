package cluster

import (
	"context"
	"fmt"
	"time"
)

// segmentGCReadIndexTimeout bounds the ReadIndex barrier used by CaughtUp so
// the caught-up gate never stalls a scrub cycle. A timeout fails closed
// (CaughtUp returns false -> GC skips this cycle), which is safe.
const segmentGCReadIndexTimeout = 5 * time.Second

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

// CaughtUp reports whether this node's applied FSM state is current enough to
// trust ListAllObjectsStrict for the GC known-set. Uses the Raft ReadIndex
// barrier (quorum-confirmed commit index) so a stale follower or deposed leader
// cannot falsely report caught-up and delete still-referenced segments. Bounded
// so it never stalls the scrub cycle; any error/timeout fails closed (returns
// false -> GC pauses this cycle, which is safe).
//
// Followers' local node.ReadIndex returns ErrNotLeader, so a follower fails
// closed and skips its segment sweep — only the leader (whose ReadIndex is
// quorum-confirmed) runs GC. Both branches are data-loss safe.
func (b *DistributedBackend) CaughtUp(ctx context.Context) bool {
	if b.node == nil {
		return true // no raft (single-node serve / tests): local state is authoritative
	}
	cctx, cancel := context.WithTimeout(ctx, segmentGCReadIndexTimeout)
	defer cancel()
	idx, err := b.node.ReadIndex(cctx)
	if err != nil {
		return false // cannot confirm freshness -> fail closed
	}
	return b.lastApplied.Load() >= idx
}
