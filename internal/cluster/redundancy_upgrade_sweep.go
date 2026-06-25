package cluster

import (
	"context"
	"errors"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/scrubber"
)

// redundancyUpgradeDeps is the dependency surface the EC redundancy-upgrade sweep
// consumes. Splitting it out keeps the selection/cap/caught-up-gate logic
// unit-testable with an in-memory fake (no real cluster).
type redundancyUpgradeDeps interface {
	// clusterRedundant reports whether the cluster currently has redundant
	// placement capacity (checked ONCE per cycle, not per object).
	clusterRedundant() bool
	// listBuckets enumerates the buckets to sweep (union of hosted groups).
	listBuckets() ([]string, error)
	// caughtUpOwner reports whether this node hosts bucket's owning group, passes
	// the GC freshness gate, and owns that group's singleton relocation role.
	caughtUpOwner(bucket string) bool
	// scanObjects streams the live EC ObjectRecords for a bucket.
	scanObjects(bucket string) (<-chan scrubber.ObjectRecord, error)
	// relocate re-encodes a non-redundant object into a redundant group. Returns
	// ErrRelocateSkipped for benign no-ops.
	relocate(ctx context.Context, in relocateInput) error
}

// runRedundancyUpgradeSweep enumerates non-redundant (1+0) EC objects across the
// fresh, singleton-owned groups this node hosts and relocates up to maxPerCycle
// of them into a redundant placement group, returning the count relocated. It
// stops early at the cap. A bucket whose owning group is not locally hosted,
// fresh, or singleton-owned here is skipped. ErrRelocateSkipped is benign
// (continue); any other relocate error is logged + counted (metric) but does
// not abort the sweep.
//
// clusterRedundant is checked once up front; needsRedundancyUpgrade is then called
// with clusterRedundant=true per object (the cluster-level capacity is already
// known — do not recompute it per object).
func runRedundancyUpgradeSweep(ctx context.Context, d redundancyUpgradeDeps, now, minAge int64, maxPerCycle int) (relocated int, err error) {
	if !d.clusterRedundant() {
		return 0, nil
	}

	buckets, err := d.listBuckets()
	if err != nil {
		return 0, err
	}

	for _, bucket := range buckets {
		if !d.caughtUpOwner(bucket) {
			continue // not locally hosted, fresh, or singleton-owned here
		}
		objCh, scanErr := d.scanObjects(bucket)
		if scanErr != nil {
			log.Warn().Str("bucket", bucket).Err(scanErr).Msg("redundancy-upgrade: scan objects failed")
			continue
		}
		// scanObjects' producer goroutine blocks on a (ctx-unaware) buffered send
		// while holding a metadata read txn, so abandoning objCh mid-range would
		// leak that goroutine + txn. On both the per-cycle cap and ctx cancellation
		// we therefore DRAIN the channel (inline for the cap so the producer
		// completes and closes; detached on cancellation so shutdown stays prompt)
		// rather than returning out of the range.
		capped := false
		for rec := range objCh {
			select {
			case <-ctx.Done():
				go drainObjectRecords(objCh)
				return relocated, nil
			default:
			}
			if capped || relocated >= maxPerCycle {
				capped = true
				continue // drain the rest of this bucket's channel; relocate no more
			}
			if !needsRedundancyUpgrade(rec, true, now, minAge) {
				continue
			}
			in := relocateInput{Bucket: rec.Bucket, Key: rec.Key, VersionID: rec.VersionID, ExpectedETag: rec.ETag}
			if rErr := d.relocate(ctx, in); rErr != nil {
				if errors.Is(rErr, ErrRelocateSkipped) {
					continue // benign: object changed / no longer a candidate
				}
				metrics.ECRedundancyUpgradeFailedTotal.Inc()
				log.Warn().Str("bucket", rec.Bucket).Str("key", rec.Key).Err(rErr).
					Msg("redundancy-upgrade: relocate failed")
				continue
			}
			metrics.ECRedundancyUpgradeRelocatedTotal.Inc()
			relocated++
		}
		if capped {
			return relocated, nil // cap reached; current bucket fully drained
		}
	}
	return relocated, nil
}

// drainObjectRecords consumes any remaining records so the ScanObjects producer
// goroutine can finish and release its metadata read txn. Used when the sweep
// stops ranging early (ctx cancellation).
func drainObjectRecords(ch <-chan scrubber.ObjectRecord) {
	for range ch {
	}
}
