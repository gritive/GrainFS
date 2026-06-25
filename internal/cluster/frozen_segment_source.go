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

// GCFreshnessGate reports whether destructive GC paths may trust this backend's
// live metadata view for the current cycle.
type GCFreshnessGate func(ctx context.Context) bool

type gcFreshnessGateHolder struct{ gate GCFreshnessGate }

type gcSingletonOwnerHolder struct{ fn func(bucket string) bool }

// SetFrozenSegmentPathSource injects the snapshot Manager's AllFrozenSegmentPaths
// so this backend can supply the snapshot half of the scrubber's known-set. Call
// once during boot, before the scrubber starts.
func (b *DistributedBackend) SetFrozenSegmentPathSource(fn func() (map[string][]string, error)) {
	b.frozenSegSrc = fn
}

// SetGCFreshnessGate overrides the legacy data-Raft ReadIndex barrier used by
// CaughtUp. nil restores the legacy behavior.
func (b *DistributedBackend) SetGCFreshnessGate(gate GCFreshnessGate) {
	if gate == nil {
		b.gcFreshnessGate.Store(nil)
		return
	}
	b.gcFreshnessGate.Store(&gcFreshnessGateHolder{gate: gate})
}

// SetGCSingletonOwnerChecker wires the predicate used by GC work that rewrites
// global metadata, such as redundancy-upgrade relocation.
func (b *DistributedBackend) SetGCSingletonOwnerChecker(fn func(bucket string) bool) {
	if fn == nil {
		b.gcSingletonOwnerFn.Store(nil)
		return
	}
	b.gcSingletonOwnerFn.Store(&gcSingletonOwnerHolder{fn: fn})
}

func (b *DistributedBackend) gcSingletonOwner(bucket string) bool {
	h := b.gcSingletonOwnerFn.Load()
	if h == nil || h.fn == nil {
		return true
	}
	return h.fn(bucket)
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

// CaughtUp reports whether this node's metadata view is current enough to trust
// ListAllObjectsStrict for the GC known-set. Production data groups wire a
// raft-free freshness gate because live object metadata is quorum-meta based.
// When un-wired, the brownfield fallback uses the Raft ReadIndex barrier
// (quorum-confirmed commit index) so a stale follower or deposed leader cannot
// falsely report caught-up and delete still-referenced segments. The fallback is
// bounded so it never stalls the scrub cycle; any error/timeout fails closed
// (returns false -> GC pauses this cycle, which is safe).
//
// In the fallback path, followers' local node.ReadIndex returns ErrNotLeader,
// so a follower fails closed and skips its segment sweep. Both branches are
// data-loss safe.
func (b *DistributedBackend) CaughtUp(ctx context.Context) bool {
	if h := b.gcFreshnessGate.Load(); h != nil && h.gate != nil {
		return h.gate(ctx)
	}
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
