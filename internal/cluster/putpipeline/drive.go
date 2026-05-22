package putpipeline

import (
	"context"
)

// DriveActor owns shard writes for one local data directory.
type DriveActor struct {
	in       chan EncryptedShardChunk
	dataDir  string
	commitCh chan<- ShardWriteResult
	pending  map[uint64]*shardWriteState
}

// registerPut tells this drive that PutID will arrive with shard
// chunks. Stores bucket/shardKey so the actor knows where the final
// shard file lives.
func (d *DriveActor) registerPut(putID uint64, bucket, shardKey string, shardIdx int) {
	// TODO Phase 3
	_ = putID
	_ = bucket
	_ = shardKey
	_ = shardIdx
}

// Run consumes from d.in until ctx is done. Survives chunk-handler
// panics by recovering and re-entering the loop.
func (d *DriveActor) Run(ctx context.Context) {
	// TODO Phase 3 — Phase 1 references silence unused-field lint.
	_ = ctx
	_ = d.in
	_ = d.dataDir
	_ = d.commitCh
	_ = d.pending
	var _ shardWriteState
}
