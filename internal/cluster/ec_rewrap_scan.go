package cluster

import (
	"strings"

	"github.com/dgraph-io/badger/v4"

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
func (b *DistributedBackend) ScanGroupObjects() <-chan scrubber.ObjectRecord {
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

				ch <- scrubber.ObjectRecord{Bucket: bucket, Key: key, VersionID: versionID}
				return nil
			})
		})
	}()
	return ch
}
