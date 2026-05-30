package cluster

import (
	"context"
	"strings"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/scrubber"
)

// ScanGroupObjects streams one scrubber.ObjectRecord per live object physically
// stored in THIS backend's data group (all buckets), by iterating the group's
// `lat:` pointers directly. Unlike ScanObjects it does NOT gate on a per-bucket
// HeadBucket: an object lives in the data group recorded in its metadata, which
// can differ from the bucket's current router assignment after a reassignment,
// and the bucket's own metadata may live in a different group. The DEK rewrap
// lane uses this to find objects where they physically are (mirroring the read
// path, which resolves an object by its stored PlacementGroupID).
//
// Delete markers are skipped. DataShards/ParityShards are left zero — the rewrap
// lane resolves shard ownership per object via OwnedShards, so the record only
// needs to identify (bucket, key, versionID).
func (b *DistributedBackend) ScanGroupObjects(ctx context.Context) <-chan scrubber.ObjectRecord {
	ch := make(chan scrubber.ObjectRecord, 64)
	go func() {
		defer close(ch)
		rawLatPrefix := []byte("lat:")
		_ = b.db.View(func(txn *badger.Txn) error {
			return b.ks().scanGroupPrefix(txn, rawLatPrefix, func(raw []byte, item *badger.Item) error {
				// raw is the group-stripped key: "lat:{bucket}/{key}".
				rest := string(raw[len(rawLatPrefix):]) // "{bucket}/{key}"
				slash := strings.IndexByte(rest, '/')
				if slash <= 0 {
					return nil
				}
				bucket := rest[:slash]
				key := rest[slash+1:]

				var versionID string
				if err := item.Value(func(v []byte) error {
					versionID = string(v)
					return nil
				}); err != nil || versionID == "" {
					return nil
				}

				metaItem, err := txn.Get(b.ks().ObjectMetaKeyV(bucket, key, versionID))
				if err != nil {
					return nil
				}
				v, err := b.itemValueCopy(metaItem)
				if err != nil {
					return nil
				}
				meta, err := unmarshalObjectMeta(v)
				if err != nil {
					return nil
				}
				if meta.ETag == deleteMarkerETag {
					return nil // tombstone — no shards to rewrap
				}

				// Honor ctx so the producer never blocks forever when the
				// consumer stops draining (e.g. ctx-cancel) — otherwise this
				// goroutine + its pinned badger read snapshot would leak.
				select {
				case ch <- scrubber.ObjectRecord{Bucket: bucket, Key: key, VersionID: versionID}:
					return nil
				case <-ctx.Done():
					return errStopScan
				}
			})
		})
	}()
	return ch
}

// SegmentShardRecord identifies a single EC-distributed segment or coalesced
// shard that is physically owned (positionally) by some node. The ShardKey is
// the canonical shard-service key (e.g. "key/segments/<blobID>" or
// "key/coalesced/<id>"). NodeIDs is positional: shard i lives on NodeIDs[i].
type SegmentShardRecord struct {
	Bucket   string
	ShardKey string
	NodeIDs  []string
}

// ScanGroupSegmentShards streams one SegmentShardRecord per EC-distributed
// segment or coalesced sub-object physically stored in THIS backend's data
// group (all buckets). It mirrors ScanGroupObjects: it iterates the group's
// `lat:` pointers directly without a HeadBucket gate, and the producer honors
// ctx so it never leaks when the consumer stops draining.
//
// Owner-local (ECData==0) and malformed refs are skipped; malformed refs with
// ECData!=0 are counted via metrics.PlacementMonitorInvalidECRef.
func (b *DistributedBackend) ScanGroupSegmentShards(ctx context.Context) <-chan SegmentShardRecord {
	ch := make(chan SegmentShardRecord, 64)
	go func() {
		defer close(ch)
		rawLatPrefix := []byte("lat:")
		_ = b.db.View(func(txn *badger.Txn) error {
			return b.ks().scanGroupPrefix(txn, rawLatPrefix, func(raw []byte, item *badger.Item) error {
				rest := string(raw[len(rawLatPrefix):]) // "{bucket}/{key}"
				slash := strings.IndexByte(rest, '/')
				if slash <= 0 {
					return nil
				}
				bucket := rest[:slash]
				key := rest[slash+1:]

				var versionID string
				if err := item.Value(func(v []byte) error {
					versionID = string(v)
					return nil
				}); err != nil || versionID == "" {
					return nil
				}

				metaItem, err := txn.Get(b.ks().ObjectMetaKeyV(bucket, key, versionID))
				if err != nil {
					return nil
				}
				v, err := b.itemValueCopy(metaItem)
				if err != nil {
					return nil
				}
				meta, err := unmarshalObjectMeta(v)
				if err != nil || meta.ETag == deleteMarkerETag {
					return nil
				}

				emit := func(shardKey string, nodeIDs []string) bool {
					select {
					case ch <- SegmentShardRecord{Bucket: bucket, ShardKey: shardKey, NodeIDs: nodeIDs}:
						return true
					case <-ctx.Done():
						return false
					}
				}

				for _, seg := range meta.Segments {
					if !validateECRefPlacement(seg.ECData, seg.ECParity, seg.NodeIDs) {
						if seg.ECData != 0 {
							metrics.PlacementMonitorInvalidECRef.WithLabelValues("segment").Inc()
						}
						continue
					}
					if !emit(key+"/segments/"+seg.BlobID, seg.NodeIDs) {
						return errStopScan
					}
				}
				for _, c := range meta.Coalesced {
					if !validateECRefPlacement(c.ECData, c.ECParity, c.NodeIDs) {
						if c.ECData != 0 {
							metrics.PlacementMonitorInvalidECRef.WithLabelValues("coalesced").Inc()
						}
						continue
					}
					if !emit(c.ShardKey, c.NodeIDs) {
						return errStopScan
					}
				}
				return nil
			})
		})
	}()
	return ch
}
