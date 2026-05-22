package putpipeline

import (
	"context"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// MetadataBatcher accumulates MetadataRecord values and commits them
// to Badger in batches sized by count or time, whichever fires first.
type MetadataBatcher struct {
	in         chan MetadataRecord
	db         *badger.DB
	batchSize  int
	flushAfter time.Duration
	pending    sync.Map // string (bucket/key/versionID) -> MetadataRecord
}

// PeekPending returns a MetadataRecord that's been queued but not yet
// committed, or false if the key isn't queued. GET reads consult this
// before hitting Badger to preserve read-after-write semantics.
func (m *MetadataBatcher) PeekPending(bucket, key, versionID string) (MetadataRecord, bool) {
	// TODO Phase 4
	_ = bucket
	_ = key
	_ = versionID
	_ = &m.pending
	return MetadataRecord{}, false
}

// Run consumes MetadataRecord values until ctx is done.
func (m *MetadataBatcher) Run(ctx context.Context) {
	// TODO Phase 4 — Phase 1 references silence unused-field lint.
	_ = ctx
	_ = m.in
	_ = m.db
	_ = m.batchSize
	_ = m.flushAfter
}
