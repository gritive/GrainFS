package cluster

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/storage/datawal"
)

// maxShardPackBatch caps records per actor commit batch. Mirrors the
// raft propose path's batching cap.
const maxShardPackBatch = 64

// maxShardPackBatchBytes caps the summed raw payload size per batch.
// Well under the data WAL's MaxPayloadBytes and the active blob's maxSize
// (256 MiB) — leaves room for the per-record header (9 B) and key bytes.
const maxShardPackBatchBytes = 4 << 20

// ErrShardPackClosed is returned by put/deleteKey after Close has been called.
var ErrShardPackClosed = errors.New("shardpack: store closed")

// commitShardPackTxn is the WAL-flush seam. Tests override it to inject failures.
// DataWALAppender is defined in the cluster package (shard_service.go:32).
var commitShardPackTxn = func(w DataWALAppender) error {
	if w == nil {
		return nil
	}
	return w.Flush()
}

// shardPackBatchMax parses a GRAINFS_SHARDPACK_BATCH_MAX value into an
// entry cap in [1, maxShardPackBatch]. Empty or invalid -> maxShardPackBatch.
// 0 is treated as 1 (batching disabled).
func shardPackBatchMax(v string) int {
	if v == "" {
		return maxShardPackBatch
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return maxShardPackBatch
	}
	if n == 0 {
		return 1
	}
	if n > maxShardPackBatch {
		return maxShardPackBatch
	}
	return n
}

// packCmd is the unit of work sent from put/deleteKey to the actor goroutine.
type packCmd struct {
	flag byte
	key  string
	data []byte
	ack  chan error // 1-buffered, caller-owned
}

// indexUpdate is the deferred index mutation collected in Phase 3a.
type indexUpdate struct {
	isDel bool
	key   string
	loc   shardPackLocation
}

// shardPackActor owns the write side of a shardPackStore. Single goroutine,
// opportunistically batches commands into one fsync + one indexMu WLock per
// batch.
type shardPackActor struct {
	store   *shardPackStore
	cap     int           // resolved from GRAINFS_SHARDPACK_BATCH_MAX
	batch   []packCmd     // reused across iterations
	updates []indexUpdate // reused across iterations
}

func newShardPackActor(s *shardPackStore) *shardPackActor {
	return &shardPackActor{
		store: s,
		cap:   shardPackBatchMax(os.Getenv("GRAINFS_SHARDPACK_BATCH_MAX")),
	}
}

// run is the actor's main loop. Returns when stopCh fires; on stop it
// drains any pending commands, commits, then signals closeAckCh.
func (a *shardPackActor) run() {
	for {
		select {
		case <-a.store.stopCh:
			a.drainAndClose()
			return
		case first := <-a.store.cmdCh:
			a.collect(first)
		}
	}
}

// collect receives one command, opportunistically drains more (no linger),
// and commits the batch.
func (a *shardPackActor) collect(first packCmd) {
	a.batch = a.batch[:0]
	a.batch = append(a.batch, first)
	batchBytes := len(first.data)

drain:
	for len(a.batch) < a.cap && batchBytes < maxShardPackBatchBytes {
		select {
		case c := <-a.store.cmdCh:
			a.batch = append(a.batch, c)
			batchBytes += len(c.data)
		default:
			break drain
		}
	}
	a.commitBatch()
}

// commitBatch runs Phase 1 (WAL append per record) + Phase 2 (one fsync) +
// Phase 3a (blob writes, collect index updates) + Phase 3b (brief WLock to
// apply index updates) + ack send.
func (a *shardPackActor) commitBatch() {
	s := a.store
	walErr := make([]error, len(a.batch))

	// Phase 1: per-record WAL append. WAL's internal mu serializes calls
	// briefly (memory append). Group commit will amortize the fsync.
	if s.dataWAL != nil {
		for i, c := range a.batch {
			record := buildShardPackRecord(c)
			op := datawal.OpShardPackPut
			if c.flag == shardPackFlagDel {
				op = datawal.OpShardPackDelete
			}
			_, err := s.dataWAL.Append(context.Background(), datawal.Record{
				Op: op, Target: c.key, Size: int64(len(record)), Payload: record,
			})
			walErr[i] = err
		}
	}

	// Phase 2: one fsync covers every record appended above.
	var flushErr error
	if s.dataWAL != nil {
		t0 := time.Now()
		flushErr = commitShardPackTxn(s.dataWAL)
		metrics.ShardPackWALFlushSeconds.Observe(time.Since(t0).Seconds())
		if flushErr != nil {
			metrics.ShardPackBatchSize.Observe(float64(len(a.batch)))
			metrics.ShardPackBatchAbortsTotal.WithLabelValues("wal_flush").Inc()
			for _, c := range a.batch {
				c.ack <- flushErr
			}
			return
		}
	}

	// Phase 3a: blob write + accumulate index updates. No lock held; readers
	// can serve get() concurrently.
	a.updates = a.updates[:0]
	results := make([]error, len(a.batch))
	aborted := false
	var abortErr error
	var abortReason string

	for i, c := range a.batch {
		if aborted {
			results[i] = abortErr
			continue
		}
		if walErr[i] != nil {
			results[i] = walErr[i]
			continue
		}
		record := buildShardPackRecord(c)
		if s.activeOff > 0 && s.activeOff+int64(len(record)) > s.maxSize {
			if err := s.rotate(); err != nil {
				results[i] = err
				aborted = true
				abortErr = err
				abortReason = "rotate"
				continue
			}
		}
		payloadOffset := s.activeOff + 9 + int64(len(c.key))
		if _, err := s.active.Write(record); err != nil {
			results[i] = err
			aborted = true
			abortErr = err
			abortReason = "blob_write"
			continue
		}
		if c.flag == shardPackFlagPut {
			a.updates = append(a.updates, indexUpdate{
				key: c.key,
				loc: shardPackLocation{
					blobID: s.activeID,
					offset: payloadOffset,
					length: uint32(len(c.data)),
				},
			})
		} else {
			a.updates = append(a.updates, indexUpdate{isDel: true, key: c.key})
		}
		s.activeOff += int64(len(record))
		results[i] = nil
	}

	// Phase 3b: brief WLock applies all index updates in one window.
	s.indexMu.Lock()
	for _, u := range a.updates {
		if u.isDel {
			for ikey := range s.index {
				if strings.HasPrefix(ikey, u.key) {
					delete(s.index, ikey)
				}
			}
		} else {
			s.index[u.key] = u.loc
		}
	}
	s.indexMu.Unlock()

	metrics.ShardPackBatchSize.Observe(float64(len(a.batch)))
	if aborted {
		metrics.ShardPackBatchAbortsTotal.WithLabelValues(abortReason).Inc()
	}
	for i, c := range a.batch {
		c.ack <- results[i]
	}
}

// drainAndClose processes any pending commands on cmdCh, commits, then signals
// closeAckCh. New enqueues after Close was called are rejected at the put/
// deleteKey fast-path via s.closed.Load().
func (a *shardPackActor) drainAndClose() {
	for {
		select {
		case c := <-a.store.cmdCh:
			a.batch = a.batch[:0]
			a.batch = append(a.batch, c)
			// Try to drain any more that are queued.
			batchBytes := len(c.data)
		drain:
			for len(a.batch) < a.cap && batchBytes < maxShardPackBatchBytes {
				select {
				case more := <-a.store.cmdCh:
					a.batch = append(a.batch, more)
					batchBytes += len(more.data)
				default:
					break drain
				}
			}
			a.commitBatch()
		default:
			a.store.closeAckCh <- nil
			return
		}
	}
}

// buildShardPackRecord builds the on-disk record bytes for a put/del command.
// Mirrors appendShardPackRecord in shard_pack.go.
func buildShardPackRecord(c packCmd) []byte {
	return appendShardPackRecord(nil, c.flag, c.key, c.data)
}
