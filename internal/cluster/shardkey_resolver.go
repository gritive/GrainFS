package cluster

import (
	"context"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// startupRepairPlacementScanCap bounds how many exact-key object versions a
// single segment/coalesced placement scan will inspect before giving up. A
// scan that exceeds the cap resolves to skipReason "placement_scan_capped".
const startupRepairPlacementScanCap = 1000

// shardKeyScanResult holds the per-(bucket,objectKey) placement maps built by a
// single version scan. Negative results are represented by empty maps (a later
// miss must not trigger a rescan), so the presence of an entry in the cache —
// not its contents — signals "already scanned".
type shardKeyScanResult struct {
	blob      map[string]PlacementRecord // segment BlobID → placement
	coalesced map[string]PlacementRecord // coalesced ShardKey → placement
	capped    bool
}

// ShardKeyPlacementScanCache memoizes one version-scan per (bucket, objectKey)
// for a single startup-repair pass. Not safe for concurrent use (the worker is
// serial).
type ShardKeyPlacementScanCache struct {
	// cap is the per-scan exact-key version cap. Seeded from
	// startupRepairPlacementScanCap; tests may override it.
	cap int
	// entries is keyed "bucket\x00objectKey". The NUL separator is collision-free
	// because S3 bucket names disallow NUL.
	entries map[string]*shardKeyScanResult
	// scans counts how many times a real version scan was performed. Test seam
	// for asserting cache reuse.
	scans int
}

// NewShardKeyPlacementScanCache returns an empty cache with the default cap.
func NewShardKeyPlacementScanCache() *ShardKeyPlacementScanCache {
	return &ShardKeyPlacementScanCache{
		cap:     startupRepairPlacementScanCap,
		entries: make(map[string]*shardKeyScanResult),
	}
}

// ResolveShardKeyPlacement resolves EC placement for any shard-key form.
//
// ObjectVersion keys resolve in O(1) via LookupObjectPlacement. Segment and
// coalesced keys have no reverse index, so placement is recovered by scanning
// the parent object's versions (obj:bucket/objectKey/) and reading the matching
// SegmentRef / CoalescedShardRef out of objectMeta. The scan is memoized per
// (bucket, objectKey) in scan when non-nil.
//
// skipReason is "" on success, else the metric label: "stale" |
// "placement_scan_capped".
func (b *DistributedBackend) ResolveShardKeyPlacement(ctx context.Context, bucket, shardKey string, scan *ShardKeyPlacementScanCache) (rec PlacementRecord, skipReason string, err error) {
	objectKey, kind, id := ClassifyStartupRepairShardKey(shardKey)

	if kind == ShardKindObjectVersion {
		rec, err := b.FSMRef().LookupObjectPlacement(bucket, objectKey, id)
		if err != nil {
			return rec, "", err
		}
		// Label empty placements (missing / non-EC object) uniformly with the
		// segment/coalesced miss path so callers see one "stale" reason.
		if len(rec.Nodes) == 0 {
			return PlacementRecord{}, "stale", nil
		}
		return rec, "", nil
	}

	res, err := b.shardKeyScanResult(ctx, bucket, objectKey, scan)
	if err != nil {
		return PlacementRecord{}, "", err
	}
	if res.capped {
		return PlacementRecord{}, "placement_scan_capped", nil
	}

	var (
		found PlacementRecord
		ok    bool
	)
	switch kind {
	case ShardKindSegment:
		found, ok = res.blob[id]
	case ShardKindCoalesced:
		// CoalescedShardRef.ShardKey stores the full physical key
		// ("<objectKey>/coalesced/<id>"), so match on shardKey, not the
		// classified id.
		found, ok = res.coalesced[shardKey]
	}
	if !ok {
		return PlacementRecord{}, "stale", nil
	}
	return found, "", nil
}

// shardKeyScanResult returns the cached scan result for (bucket, objectKey),
// performing the scan on a cache miss. When scan is nil it scans without
// caching.
func (b *DistributedBackend) shardKeyScanResult(ctx context.Context, bucket, objectKey string, scan *ShardKeyPlacementScanCache) (*shardKeyScanResult, error) {
	capLimit := startupRepairPlacementScanCap
	if scan != nil {
		capLimit = scan.cap
		if cached, ok := scan.entries[bucket+"\x00"+objectKey]; ok {
			return cached, nil
		}
	}

	res, err := b.scanShardKeyPlacement(ctx, bucket, objectKey, capLimit)
	if err != nil {
		return nil, err
	}
	if scan != nil {
		scan.scans++
		scan.entries[bucket+"\x00"+objectKey] = res
	}
	return res, nil
}

// scanShardKeyPlacement iterates obj:bucket/objectKey/ and builds the segment
// and coalesced placement maps from every object version whose meta.Key exactly
// equals objectKey. Versions whose meta.Key only shares the prefix (nested
// siblings) are ignored and do not count toward the cap.
func (b *DistributedBackend) scanShardKeyPlacement(ctx context.Context, bucket, objectKey string, capLimit int) (*shardKeyScanResult, error) {
	res := &shardKeyScanResult{
		blob:      make(map[string]PlacementRecord),
		coalesced: make(map[string]PlacementRecord),
	}
	rawObjPfx := []byte("obj:" + bucket + "/" + objectKey + "/")
	objPrefix := b.ks().Prefix(rawObjPfx)

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = objPrefix
		it := txn.NewIterator(opts)
		defer it.Close()
		exact := 0
		for it.Seek(objPrefix); it.ValidForPrefix(objPrefix); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			val, err := b.itemValueCopy(it.Item())
			if err != nil {
				return fmt.Errorf("read object meta value: %w", err)
			}
			m, err := unmarshalObjectMeta(val)
			if err != nil {
				return fmt.Errorf("unmarshal object meta: %w", err)
			}
			if m.Key != objectKey {
				continue
			}
			exact++
			if exact > capLimit {
				res.capped = true
				return nil
			}
			for _, ref := range m.Segments {
				if ref.ECData == 0 {
					// Owner-local append blob, not EC-distributed.
					continue
				}
				res.blob[ref.BlobID] = PlacementRecord{
					// Aliasing ref.NodeIDs is safe: unmarshalObjectMeta builds
					// NodeIDs as Go-owned string copies, not FlatBuffer-backed
					// views, so the cached record aliases no transient buffer.
					Nodes: ref.NodeIDs,
					K:     int(ref.ECData),
					M:     int(ref.ECParity),
				}
			}
			for _, ref := range m.Coalesced {
				res.coalesced[ref.ShardKey] = PlacementRecord{
					Nodes: ref.NodeIDs,
					K:     int(ref.ECData),
					M:     int(ref.ECParity),
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}
