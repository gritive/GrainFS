package cluster

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/scrubber"
)

// SetOnScrubTrigger registers a callback fired when a non-stale
// MetaScrubTriggerCmd is applied. Stale entries (requested_at > scrubTriggerMaxAge
// ago) are skipped with a log line so fresh nodes joining via raft replay do
// not re-run ancient scrubs. Must not block — the callback runs on the apply
// loop.
func (f *MetaFSM) SetOnScrubTrigger(fn func(scrubber.ScrubTriggerEntry)) {
	f.mu.Lock()
	f.onScrubTrigger = fn
	f.mu.Unlock()
}

// scrubTriggerMaxAge bounds how old a replayed trigger may be before it's
// silently dropped. Triggers within this window apply normally on every node.
const scrubTriggerMaxAge = 1 * time.Hour

func (f *MetaFSM) applyScrubTrigger(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: applyScrubTrigger: empty payload")
	}
	var (
		c      *clusterpb.MetaScrubTriggerCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaScrubTriggerCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaScrubTriggerCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}
	if c == nil {
		return fmt.Errorf("meta_fsm: applyScrubTrigger: nil cmd")
	}
	requestedAt := time.Unix(c.RequestedAt(), 0)
	if time.Since(requestedAt) > scrubTriggerMaxAge {
		log.Warn().
			Str("session_id", string(c.SessionId())).
			Str("bucket", string(c.Bucket())).
			Time("requested_at", requestedAt).
			Msg("meta_fsm: applyScrubTrigger skipping stale entry (>1h old)")
		return nil
	}
	f.mu.RLock()
	cb := f.onScrubTrigger
	f.mu.RUnlock()
	if cb == nil {
		return nil
	}
	cb(scrubber.ScrubTriggerEntry{
		SessionID:        string(c.SessionId()),
		Bucket:           string(c.Bucket()),
		KeyPrefix:        string(c.KeyPrefix()),
		DryRun:           c.DryRun(),
		RequestedAt:      c.RequestedAt(),
		OriginatorNodeID: string(c.OriginatorNodeId()),
	})
	return nil
}

func encodeMetaScrubTriggerCmd(entry scrubber.ScrubTriggerEntry) ([]byte, error) {
	b := clusterBuilderPool.Get()
	sidOff := b.CreateString(entry.SessionID)
	bktOff := b.CreateString(entry.Bucket)
	pfxOff := b.CreateString(entry.KeyPrefix)
	nodeOff := b.CreateString(entry.OriginatorNodeID)
	clusterpb.MetaScrubTriggerCmdStart(b)
	clusterpb.MetaScrubTriggerCmdAddSessionId(b, sidOff)
	clusterpb.MetaScrubTriggerCmdAddBucket(b, bktOff)
	clusterpb.MetaScrubTriggerCmdAddKeyPrefix(b, pfxOff)
	clusterpb.MetaScrubTriggerCmdAddDryRun(b, entry.DryRun)
	clusterpb.MetaScrubTriggerCmdAddRequestedAt(b, entry.RequestedAt)
	clusterpb.MetaScrubTriggerCmdAddOriginatorNodeId(b, nodeOff)
	return fbFinish(b, clusterpb.MetaScrubTriggerCmdEnd(b)), nil
}
