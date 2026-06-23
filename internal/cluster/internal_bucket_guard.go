package cluster

import (
	"context"
	"errors"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/storage"
)

// ErrInternalBucketNotObjectStore is returned when an object data-plane
// operation targets an internal (__grainfs_) bucket. Object storage on
// internal buckets has been removed as a supported capability.
var ErrInternalBucketNotObjectStore = errors.New("internal bucket is not an object store")

// guardInternalBucketObjectOp returns ErrInternalBucketNotObjectStore when
// bucket is an internal GrainFS bucket, blocking all object data-plane ops.
func guardInternalBucketObjectOp(bucket string) error {
	if storage.IsInternalBucket(bucket) {
		return ErrInternalBucketNotObjectStore
	}
	return nil
}

// WarnOnInternalBucketObjects scans for pre-existing internal-bucket objects
// (those that existed before the capability was removed) and emits a WARN log
// plus a metric for each affected bucket. On a greenfield deployment this is a
// no-op. Uses a raw metadata scan so it does not go through the guarded API.
func (b *DistributedBackend) WarnOnInternalBucketObjects(ctx context.Context) {
	buckets, err := b.ListBuckets(ctx)
	if err != nil {
		return
	}
	for _, name := range buckets {
		if !storage.IsInternalBucket(name) {
			continue
		}
		if b.internalBucketHasAnyObject(name) {
			log.Warn().Str("bucket", name).
				Msg("internal-bucket object found; unreachable after capability removal — manual cleanup required")
			metrics.InternalBucketOrphanObjectsTotal.Inc()
		}
	}
}

// internalBucketHasAnyObject performs a raw metadata scan for obj: or lat:
// keys under the given internal bucket. Returns true as soon as one key is
// found. The scan goes through the FSM store directly, bypassing the guarded
// object API, so it remains correct even after the guard is in place.
func (b *DistributedBackend) internalBucketHasAnyObject(bucket string) bool {
	found := false
	_ = b.store.View(func(txn MetadataTxn) error {
		// Scan obj:{bucket}/ prefix first.
		objPrefix := []byte("obj:" + bucket + "/")
		if err := b.ks().scanGroupPrefix(txn, objPrefix, func(_ []byte, _ MetaItem) error {
			found = true
			return errStopScan
		}); err != nil {
			return err
		}
		if found {
			return nil
		}
		// Scan lat:{bucket}/ prefix.
		latPrefix := []byte("lat:" + bucket + "/")
		return b.ks().scanGroupPrefix(txn, latPrefix, func(_ []byte, _ MetaItem) error {
			found = true
			return errStopScan
		})
	})
	return found
}
