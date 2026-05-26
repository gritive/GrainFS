package cluster

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

func buildPutObjectMeta(cmd PutObjectMetaCmd) objectMeta {
	etag := cmd.ETag
	if cmd.IsDeleteMarker {
		etag = deleteMarkerETag
	}
	return objectMeta{
		Key:              cmd.Key,
		Size:             cmd.Size,
		ContentType:      cmd.ContentType,
		ETag:             etag,
		LastModified:     cmd.ModTime,
		ECData:           cmd.ECData,
		ECParity:         cmd.ECParity,
		NodeIDs:          cmd.NodeIDs,
		PlacementGroupID: cmd.PlacementGroupID,
		UserMetadata:     cmd.UserMetadata,
		SSEAlgorithm:     cmd.SSEAlgorithm,
		Parts:            cmd.Parts,
		Segments:         segmentMetaEntriesToRefs(cmd.Segments),
		Tags:             cmd.Tags,
	}
}

func (f *FSM) checkPutObjectExpectedETag(txn *badger.Txn, bucket, key, expectedETag string) error {
	if expectedETag == "" {
		return nil
	}
	item, err := txn.Get(f.keys.ObjectMetaKey(bucket, key))
	if err != nil {
		return fmt.Errorf("put object meta CAS: read current meta: %w", err)
	}
	return item.Value(func(val []byte) error {
		current, err := unmarshalObjectMeta(val)
		if err != nil {
			return fmt.Errorf("put object meta CAS: decode current meta: %w", err)
		}
		if current.ETag != expectedETag {
			return fmt.Errorf("put object meta CAS: etag changed for %s/%s: got %q, want %q",
				bucket, key, current.ETag, expectedETag)
		}
		return nil
	})
}
