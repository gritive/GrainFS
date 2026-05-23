package putpipeline

import (
	"context"
	"fmt"
	"sync"
)

// CommitCoord aggregates ShardWriteResults from all DriveActors and
// gates the handler's HTTP response on K-of-N data-shard completion.
// One long-lived goroutine per server.
type CommitCoord struct {
	in          chan ShardWriteResult
	metaBatchCh chan<- MetadataRecord
	waiters     map[uint64]*putWaiter
	mu          sync.Mutex
	wal         ShardWALAppender // optional; when set, called once per PUT before final-done
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
			// WAL-only fsync (commit b508c73d contract): pay one fsync
			// for the whole PUT inside the WAL, in place of N per-shard
			// fsyncs in DriveActor. The on-disk rename has already
			// happened by the time a ShardWriteResult arrives, so any
			// shard the WAL covers also has its file in place.
			if c.wal != nil {
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
				if len(records) > 0 {
					if _, err := c.wal.AppendBatch(context.Background(), records); err != nil {
						w.finalDone <- fmt.Errorf(
							"put %d: wal append batch: %w", res.PutID, err)
						c.mu.Lock()
						delete(c.waiters, res.PutID)
						c.mu.Unlock()
						return
					}
				}
			}
			c.metaBatchCh <- w.metadata
			w.finalDone <- nil
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
