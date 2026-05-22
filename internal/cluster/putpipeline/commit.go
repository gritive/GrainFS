package putpipeline

import (
	"context"
)

// CommitCoord aggregates ShardWriteResults from all DriveActors and
// gates the handler's HTTP response on K-of-N data-shard completion.
type CommitCoord struct {
	in          chan ShardWriteResult
	metaBatchCh chan<- MetadataRecord
	waiters     map[uint64]*putWaiter
}

// registerPut wires per-PUT response channels.
func (c *CommitCoord) registerPut(putID uint64, w *putWaiter) {
	// TODO Phase 4
	_ = putID
	_ = w
}

// Run consumes ShardWriteResults until ctx is done.
func (c *CommitCoord) Run(ctx context.Context) {
	// TODO Phase 4 — Phase 1 references silence unused-field lint.
	_ = ctx
	_ = c.in
	_ = c.metaBatchCh
	_ = c.waiters
	var _ putWaiter
}
