package cluster

import (
	"context"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/scrubber"
)

// ScanAppendableObjects streams an AppendableRecord for each IsAppendable
// object in `bucket`. Used by the scrubber to build the known-segment set
// for orphan sweep. Mirrors ScanObjects' lat: iteration pattern but filters
// for IsAppendable=true and emits all SegmentBlobIDs from obj.Segments.
func (b *DistributedBackend) ScanAppendableObjects(bucket string) (<-chan scrubber.AppendableRecord, error) {
	if err := b.HeadBucket(context.Background(), bucket); err != nil {
		return nil, err
	}
	ch := make(chan scrubber.AppendableRecord, 64)
	go func() {
		defer close(ch)
		rawLatPrefix := []byte("lat:" + bucket + "/")

		_ = b.db.View(func(txn *badger.Txn) error {
			return b.ks().scanGroupPrefix(txn, rawLatPrefix, func(raw []byte, item *badger.Item) error {
				key := string(raw[len(rawLatPrefix):])

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
					return nil
				}
				if !meta.IsAppendable {
					return nil
				}
				blobIDs := make([]string, 0, len(meta.Segments))
				for _, seg := range meta.Segments {
					blobIDs = append(blobIDs, seg.BlobID)
				}
				ch <- scrubber.AppendableRecord{
					Bucket:         bucket,
					Key:            key,
					SegmentBlobIDs: blobIDs,
				}
				return nil
			})
		})
	}()
	return ch, nil
}
