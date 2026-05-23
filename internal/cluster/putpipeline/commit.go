package putpipeline

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

// CommitCoord aggregates ShardWriteResults from all DriveActors and
// gates the handler's HTTP response on K-of-N data-shard completion.
// One long-lived goroutine per server.
//
// When a WAL is wired, a separate walFlusher goroutine batches WAL
// fsyncs across concurrent PUTs ("group commit"): the dominant cost
// of per-PUT WAL.Flush serialized 16-concurrent PUTs through a single
// CommitCoord goroutine, capping throughput at ~31 PUTs/s × stripe-
// count. Group commit dissolves that serialization — N PUTs share one
// fsync.
type CommitCoord struct {
	in          chan ShardWriteResult
	metaBatchCh chan<- MetadataRecord
	waiters     map[uint64]*putWaiter
	mu          sync.Mutex
	wal         ShardWALAppender // optional; when set, walIn carries records to walFlusher
	walIn       chan walFlushItem
}

// walFlushItem is what CommitCoord hands off to walFlusher when a PUT's
// last shard has hit disk. walFlusher batches multiple items into one
// fsync, then forwards metadata + signals finalDone for every item in
// the batch.
type walFlushItem struct {
	records   []ShardWALRecord
	metadata  MetadataRecord
	finalDone chan<- error
}

// Group-commit tuning. Defaults: 32-PUT batch, 500µs window. Chosen
// from a conc=16/32 × batch×wait sweep on M3 single-node 4-drive:
// these defaults sit at the throughput peak for both conc=16 (~749
// MiB/s) and conc=32 (~784 MiB/s) on 10 MiB PUT. Override via
// GRAINFS_WAL_BATCH / GRAINFS_WAL_WAIT_US for environment-specific
// tuning (parsed once at package init).
//
// The 500µs window adds at most ~1.7% latency at conc=1 (one PUT ≈
// 30 ms), and gives walFlusher enough time to gather a non-trivial
// batch at conc=4-8 where size-trigger alone wouldn't fire.
var (
	walFlushMaxBatch = envInt("GRAINFS_WAL_BATCH", 32)
	walFlushMaxWait  = time.Duration(envInt("GRAINFS_WAL_WAIT_US", 500)) * time.Microsecond
)

func envInt(name string, defaultVal int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return defaultVal
}

func (c *CommitCoord) registerPut(putID uint64, w *putWaiter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.waiters[putID] = w
}

func (c *CommitCoord) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case res, ok := <-c.in:
			if !ok {
				return
			}
			c.handle(res)
		}
	}
}

func (c *CommitCoord) handle(res ShardWriteResult) {
	c.mu.Lock()
	w, ok := c.waiters[res.PutID]
	c.mu.Unlock()
	if !ok {
		return
	}

	if res.Err != nil {
		w.shardsFailed++
	} else {
		w.shardsOK++
		if res.ShardIdx < w.cfg.DataShards {
			w.dataShardsOK++
		}
		for len(w.metadata.ShardSizes) <= res.ShardIdx {
			w.metadata.ShardSizes = append(w.metadata.ShardSizes, 0)
		}
		w.metadata.ShardSizes[res.ShardIdx] = res.Bytes
	}

	// Early-ack gate must run before the final-gate block so that the
	// last-arriving failure still sends earlyAck on the same call.
	if !w.earlyAckSent {
		if w.dataShardsOK >= w.cfg.DataShards {
			w.earlyAck <- nil
			w.earlyAckSent = true
		} else if !canStillReachK(w) {
			w.earlyAck <- fmt.Errorf(
				"put %d: K data shards unreachable (ok=%d failed=%d need=%d)",
				res.PutID, w.dataShardsOK, w.shardsFailed, w.cfg.DataShards)
			w.earlyAckSent = true
		}
	}

	if w.shardsOK+w.shardsFailed >= w.shardsTotal {
		if w.dataShardsOK >= w.cfg.DataShards {
			// WAL-only fsync (commit b508c73d): pay one WAL fsync per
			// PUT in place of N per-shard fsyncs. With group commit the
			// fsync is further amortized across concurrent PUTs by
			// walFlusher.
			if c.wal != nil && c.walIn != nil {
				records := make([]ShardWALRecord, 0, len(w.metadata.ShardSizes))
				for i, sz := range w.metadata.ShardSizes {
					if sz <= 0 {
						continue
					}
					records = append(records, ShardWALRecord{
						Bucket:   w.metadata.Bucket,
						Key:      w.metadata.Key,
						ShardIdx: i,
						Size:     sz,
					})
				}
				// Hand off to walFlusher; it owns metaBatchCh send and
				// finalDone signal for every WAL-bound PUT. CommitCoord
				// returns to its loop immediately.
				c.walIn <- walFlushItem{
					records:   records,
					metadata:  w.metadata,
					finalDone: w.finalDone,
				}
			} else {
				c.metaBatchCh <- w.metadata
				w.finalDone <- nil
			}
		} else {
			w.finalDone <- fmt.Errorf(
				"put %d: insufficient shards (ok=%d failed=%d)",
				res.PutID, w.shardsOK, w.shardsFailed)
		}
		c.mu.Lock()
		delete(c.waiters, res.PutID)
		c.mu.Unlock()
	}
}

// walFlusher batches WAL fsyncs across concurrent PUTs. Triggers a
// flush when either walFlushMaxBatch items have accumulated OR
// walFlushMaxWait has elapsed since the first item in the current
// batch. Each batched fsync ack-completes every item in the batch.
//
// Flush failure fan-out: every waiter in the batch receives the same
// error on its finalDone channel, and none of their metadata records
// reach the MetadataBatcher — the PUT is reported as failed.
func (c *CommitCoord) walFlusher(ctx context.Context) {
	if c.wal == nil || c.walIn == nil {
		return
	}
	var pending []walFlushItem
	var flushTimer *time.Timer
	var flushDeadline <-chan time.Time

	flush := func() {
		if len(pending) == 0 {
			return
		}
		batch := pending
		pending = nil
		flushDeadline = nil
		c.flushBatch(ctx, batch)
	}

	for {
		select {
		case <-ctx.Done():
			flush() // best-effort drain
			return
		case item, ok := <-c.walIn:
			if !ok {
				flush()
				return
			}
			pending = append(pending, item)
			if len(pending) == 1 {
				if flushTimer == nil {
					flushTimer = time.NewTimer(walFlushMaxWait)
				} else {
					flushTimer.Reset(walFlushMaxWait)
				}
				flushDeadline = flushTimer.C
			}
			if len(pending) >= walFlushMaxBatch {
				if flushTimer != nil && !flushTimer.Stop() {
					select {
					case <-flushTimer.C:
					default:
					}
				}
				flush()
			}
		case <-flushDeadline:
			flush()
		}
	}
}

func (c *CommitCoord) flushBatch(ctx context.Context, batch []walFlushItem) {
	totalRecords := 0
	for i := range batch {
		totalRecords += len(batch[i].records)
	}
	all := make([]ShardWALRecord, 0, totalRecords)
	for i := range batch {
		all = append(all, batch[i].records...)
	}
	var flushErr error
	if len(all) > 0 {
		if _, err := c.wal.AppendBatch(ctx, all); err != nil {
			flushErr = fmt.Errorf("wal group commit: %w", err)
		}
	}
	for i := range batch {
		if flushErr != nil {
			batch[i].finalDone <- flushErr
			continue
		}
		c.metaBatchCh <- batch[i].metadata
		batch[i].finalDone <- nil
	}
}

// canStillReachK reports whether enough not-yet-reported shards remain
// that the PUT could still reach K successful data shards. Conservative:
// counts every remaining shard as a potential data shard (capped by the
// number of data shards still needed).
func canStillReachK(w *putWaiter) bool {
	remaining := w.shardsTotal - w.shardsOK - w.shardsFailed
	dataNeeded := w.cfg.DataShards - w.dataShardsOK
	maxDataLeft := remaining
	if maxDataLeft > dataNeeded {
		maxDataLeft = dataNeeded
	}
	return w.dataShardsOK+maxDataLeft >= w.cfg.DataShards
}
