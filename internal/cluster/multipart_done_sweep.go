package cluster

import (
	"context"
	"strings"
	"time"
)

// readDoneMarker reads the mpudone marker for uploadID from the local store.
// Returns (nil, nil) when the marker does not exist (no completion recorded yet).
// Used by the phantom-winner guard in commitCompleteMultipartObjectWriteResult.
func (b *DistributedBackend) readDoneMarker(uploadID string) (*multipartDone, error) {
	var marker *multipartDone
	if err := b.store.View(func(txn MetadataTxn) error {
		item, err := txn.Get(b.ks().MultipartDoneKey(uploadID))
		if err == ErrMetaKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		raw, err := b.itemValueCopy(item)
		if err != nil {
			return err
		}
		m, err := unmarshalMultipartDone(raw)
		if err != nil {
			return err
		}
		marker = &m
		return nil
	}); err != nil {
		return nil, err
	}
	return marker, nil
}

// SweepStaleMultipartDoneMarkers scans the local mpudone: keyspace for markers
// older than minAge and proposes a CmdDeleteMultipartDone batch to GC them.
// At most maxPerCycle upload IDs are collected per call. Returns the count
// included in the batch (0 when no stale markers exist).
//
// Leader-gating: this runs leader-only. Correctness never required it — b.propose
// forwards every node's deletes to the raft leader, who dedups via idempotent
// CmdDeleteMultipartDone — but the mpudone: keyspace is fully replicated meta-Raft
// state, so the leader already holds every marker. Letting followers also scan the
// whole keyspace and run the per-version blob-durability probes (phase 2, FS/peer
// reads) every cycle is pure N-way redundant work whose proposals just forward back
// to the leader. Gating on b.node.IsLeader() keeps the leader's authoritative scan
// and drops the waste. Single-node mode (b.node == nil) always runs.
//
// Implements scrubber.MultipartDoneSweeper.
func (b *DistributedBackend) SweepStaleMultipartDoneMarkers(ctx context.Context, maxPerCycle int, minAge time.Duration) (int, error) {
	if maxPerCycle <= 0 {
		return 0, nil
	}
	if b.node != nil && !b.node.IsLeader() {
		return 0, nil
	}
	now := time.Now()

	// Phase 1 (in txn): collect age-eligible candidates. The per-version blob
	// durability probe (phase 2) does FS/peer reads and must run OUTSIDE this View.
	type candidate struct {
		uploadID, bucket, key, versionID string
		hasMetaBlob                      bool
	}
	var candidates []candidate
	if err := b.store.View(func(txn MetadataTxn) error {
		return b.ks().scanGroupPrefix(txn, []byte("mpudone:"), func(rawKey []byte, item MetaItem) error {
			if len(candidates) >= maxPerCycle {
				return errStopScan
			}
			raw, err := b.itemValueCopy(item)
			if err != nil {
				// Skip single corrupt item; do not abort the batch.
				return nil
			}
			marker, err := unmarshalMultipartDone(raw)
			if err != nil {
				// Skip single corrupt marker; do not abort the batch.
				return nil
			}
			age := now.Sub(time.Unix(marker.ModTime, 0))
			if age > minAge {
				candidates = append(candidates, candidate{
					uploadID:    strings.TrimPrefix(string(rawKey), "mpudone:"),
					bucket:      marker.Bucket,
					key:         marker.Key,
					versionID:   marker.VersionID,
					hasMetaBlob: len(marker.MetaBlob) > 0,
				})
			}
			return nil
		})
	}); err != nil {
		return 0, err
	}

	// Phase 2 (out of txn): a blob-authoritative marker (meta_blob present) is the
	// ONLY copy of the winning object metadata until its per-version blob is durable
	// cluster-wide. Probe it; keep the marker (skip the GC) if the blob is absent or
	// the probe errors. Non-meta_blob (legacy/non-versioned) markers are swept on the
	// age gate alone, exactly as before.
	var stale []string
	for _, c := range candidates {
		if c.hasMetaBlob {
			if _, ok, perr := b.readQuorumMetaVersionDecodeStrict(c.bucket, c.key, c.versionID); perr != nil || !ok {
				continue // not yet durable (or unreadable) → keep the marker
			}
		}
		stale = append(stale, c.uploadID)
	}

	if len(stale) == 0 {
		return 0, nil
	}
	if err := b.propose(ctx, CmdDeleteMultipartDone, DeleteMultipartDoneCmd{UploadIDs: stale}); err != nil {
		return 0, err
	}
	return len(stale), nil
}
