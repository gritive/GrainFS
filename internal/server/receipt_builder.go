package server

import (
	"github.com/google/uuid"

	"github.com/gritive/GrainFS/internal/receipt"
	"github.com/gritive/GrainFS/internal/scrubber"
)

// buildReceipt assembles a HealReceipt from the session's events.
func buildReceipt(correlationID string, events []scrubber.HealEvent) *receipt.HealReceipt {
	if len(events) == 0 {
		return nil
	}

	first := events[0]
	var (
		shardsLost    []int32
		shardsRebuilt []int32
		eventIDs      []string
		peers         []string
		peerSet       = make(map[string]struct{})
		startTime     = first.Timestamp
		endTime       = first.Timestamp
		bytesRepaired int64
	)

	for _, ev := range events {
		eventIDs = append(eventIDs, ev.ID)
		if ev.Timestamp.Before(startTime) {
			startTime = ev.Timestamp
		}
		if ev.Timestamp.After(endTime) {
			endTime = ev.Timestamp
		}
		if ev.PeerID != "" {
			if _, seen := peerSet[ev.PeerID]; !seen {
				peerSet[ev.PeerID] = struct{}{}
				peers = append(peers, ev.PeerID)
			}
		}
		bytesRepaired += ev.BytesRepaired
		switch ev.Phase {
		case scrubber.PhaseDetect:
			if ev.ShardID >= 0 {
				shardsLost = append(shardsLost, ev.ShardID)
			}
		case scrubber.PhaseWrite:
			if ev.Outcome == scrubber.OutcomeSuccess && ev.ShardID >= 0 {
				shardsRebuilt = append(shardsRebuilt, ev.ShardID)
			}
		}
	}

	durationMs := uint32(endTime.Sub(startTime).Milliseconds())

	return &receipt.HealReceipt{
		ReceiptID:     uuid.Must(uuid.NewV7()).String(),
		Timestamp:     startTime,
		Object:        receipt.ObjectRef{Bucket: first.Bucket, Key: first.Key, VersionID: first.VersionID},
		ShardsLost:    shardsLost,
		ShardsRebuilt: shardsRebuilt,
		PeersInvolved: peers,
		DurationMs:    durationMs,
		EventIDs:      eventIDs,
		CorrelationID: correlationID,
	}
}
