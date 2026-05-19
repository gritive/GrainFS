package cluster

import (
	"context"
	"fmt"
	"io"

	"github.com/gritive/GrainFS/internal/storage"
)

type clusterSegmentStore struct {
	b      *DistributedBackend
	bucket string
	key    string
	obj    *storage.Object
}

func (s *clusterSegmentStore) OpenSegment(ctx context.Context, ref storage.SegmentRef) (io.ReadCloser, error) {
	entry, ok := s.segmentRef(ref.BlobID)
	if !ok {
		return nil, fmt.Errorf("segment %s not found in metadata for %s/%s", ref.BlobID, s.bucket, s.key)
	}
	if entry.Size <= 0 {
		return nil, fmt.Errorf("segment %s has invalid size %d", entry.BlobID, entry.Size)
	}

	record, err := s.placementRecord(entry)
	if err != nil {
		return nil, err
	}

	shardKey := s.key + "/segments/" + entry.BlobID
	return s.b.getObjectECReaderAtShardKey(ctx, s.bucket, shardKey, record, entry.Size)
}

func (s *clusterSegmentStore) segmentRef(blobID string) (storage.SegmentRef, bool) {
	if s.obj == nil {
		return storage.SegmentRef{}, false
	}
	for _, seg := range s.obj.Segments {
		if seg.BlobID == blobID {
			return seg, true
		}
	}
	return storage.SegmentRef{}, false
}

func (s *clusterSegmentStore) placementRecord(ref storage.SegmentRef) (PlacementRecord, error) {
	if len(ref.NodeIDs) > 0 && ref.ECData > 0 {
		return PlacementRecord{
			Nodes: cloneStringSlice(ref.NodeIDs),
			K:     int(ref.ECData),
			M:     int(ref.ECParity),
		}, nil
	}

	return PlacementRecord{}, fmt.Errorf("segment %s missing EC placement metadata for %s/%s", ref.BlobID, s.bucket, s.key)
}

func chunkedSegmentWindow(refs []storage.SegmentRef, offset int64, length int) ([]storage.SegmentRef, int64, error) {
	if length == 0 {
		return nil, 0, nil
	}
	var cur int64
	for startIdx, ref := range refs {
		next := cur + ref.Size
		if offset < next {
			startOff := offset - cur
			remaining := int64(length)
			for endIdx := startIdx; endIdx < len(refs); endIdx++ {
				available := refs[endIdx].Size
				if endIdx == startIdx {
					available -= startOff
				}
				remaining -= available
				if remaining <= 0 {
					return refs[startIdx : endIdx+1], startOff, nil
				}
			}
			return refs[startIdx:], startOff, nil
		}
		cur = next
	}
	return nil, 0, io.EOF
}

func segmentMetaEntriesToRefs(entries []SegmentMetaEntry) []storage.SegmentRef {
	if len(entries) == 0 {
		return nil
	}
	refs := make([]storage.SegmentRef, len(entries))
	for i, entry := range entries {
		refs[i] = storage.SegmentRef{
			BlobID:           entry.BlobID,
			Size:             entry.Size,
			Checksum:         append([]byte(nil), entry.Checksum...),
			PlacementGroupID: entry.PlacementGroupID,
			ShardSize:        entry.ShardSize,
			RingVersion:      uint64(entry.RingVersion),
			ECData:           entry.ECData,
			ECParity:         entry.ECParity,
			NodeIDs:          cloneStringSlice(entry.NodeIDs),
		}
	}
	return refs
}
