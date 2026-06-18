package cluster

import (
	"context"
	"strings"
	"time"
)

// SweepStaleMultipartDoneMarkers scans the local mpudone: keyspace for markers
// older than minAge and proposes a CmdDeleteMultipartDone batch to GC them.
// At most maxPerCycle upload IDs are collected per call. Returns the count
// included in the batch (0 when no stale markers exist).
//
// Leader-gating: b.propose forwards to the raft leader automatically, so no
// explicit IsLeader check is required — every node may call this. Only the
// leader commits the batch; followers' proposals are forwarded and, on success,
// replicated back, so the deletion is consistent across the cluster.
//
// Implements scrubber.MultipartDoneSweeper.
func (b *DistributedBackend) SweepStaleMultipartDoneMarkers(ctx context.Context, maxPerCycle int, minAge time.Duration) (int, error) {
	if maxPerCycle <= 0 {
		return 0, nil
	}
	now := time.Now()
	var stale []string

	_ = b.store.View(func(txn MetadataTxn) error {
		return b.ks().scanGroupPrefix(txn, []byte("mpudone:"), func(rawKey []byte, item MetaItem) error {
			if len(stale) >= maxPerCycle {
				return errStopScan
			}
			raw, err := b.itemValueCopy(item)
			if err != nil {
				return err
			}
			marker, err := unmarshalMultipartDone(raw)
			if err != nil {
				return err
			}
			age := now.Sub(time.Unix(marker.ModTime, 0))
			if age > minAge {
				uploadID := strings.TrimPrefix(string(rawKey), "mpudone:")
				stale = append(stale, uploadID)
			}
			return nil
		})
	})

	if len(stale) == 0 {
		return 0, nil
	}
	if err := b.propose(ctx, CmdDeleteMultipartDone, DeleteMultipartDoneCmd{UploadIDs: stale}); err != nil {
		return 0, err
	}
	return len(stale), nil
}
