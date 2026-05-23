package putpipeline

import (
	"context"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// MetadataBatcher accumulates MetadataRecord values and commits them
// to Badger in batches sized by count or time, whichever fires first.
// One long-lived goroutine per server.
type MetadataBatcher struct {
	in         chan MetadataRecord
	db         *badger.DB
	batchSize  int
	flushAfter time.Duration
	pending    sync.Map // pendingKey -> MetadataRecord

	// flushFn is overridable for tests; nil means use m.flush.
	flushFn func([]MetadataRecord)
}

type pendingKey struct{ Bucket, Key, VersionID string }

// PeekPending returns a MetadataRecord queued but not yet committed to
// Badger, or false if the key is not queued. GET reads consult this
// before hitting Badger so read-after-write semantics hold while the
// batch is in flight.
func (m *MetadataBatcher) PeekPending(bucket, key, versionID string) (MetadataRecord, bool) {
	v, ok := m.pending.Load(pendingKey{Bucket: bucket, Key: key, VersionID: versionID})
	if !ok {
		return MetadataRecord{}, false
	}
	return v.(MetadataRecord), true
}

// Run consumes MetadataRecord values until ctx is done, flushing in
// batches by count or by time.
func (m *MetadataBatcher) Run(ctx context.Context) {
	flush := m.flushFn
	if flush == nil {
		flush = m.flush
	}
	buf := make([]MetadataRecord, 0, m.batchSize)
	timer := time.NewTimer(m.flushAfter)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			if len(buf) > 0 {
				flush(buf)
			}
			return
		case rec, ok := <-m.in:
			if !ok {
				if len(buf) > 0 {
					flush(buf)
				}
				return
			}
			m.pending.Store(
				pendingKey{Bucket: rec.Bucket, Key: rec.Key, VersionID: rec.VersionID},
				rec,
			)
			buf = append(buf, rec)
			if len(buf) >= m.batchSize {
				flush(buf)
				buf = buf[:0]
				resetTimer(timer, m.flushAfter)
			}
		case <-timer.C:
			if len(buf) > 0 {
				flush(buf)
				buf = buf[:0]
			}
			timer.Reset(m.flushAfter)
		}
	}
}

func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

// flush commits a batch to Badger. Phase 5 wires the real Badger txn;
// here (db == nil in unit tests) it is a no-op, leaving the pending
// entries visible to PeekPending, which is what
// TestMetadataBatcher_PendingVisibleBeforeCommit validates.
func (m *MetadataBatcher) flush(buf []MetadataRecord) {
	if m.db == nil {
		return
	}
	// Phase 5: real Badger Update txn + pending.Delete on success.
}
