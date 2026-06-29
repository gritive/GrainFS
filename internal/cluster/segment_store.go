package cluster

import (
	"bytes"
	"context"
	"errors"
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
	loc := storage.ParseLocator(ref.BlobID)
	if loc.Scheme == storage.LocatorCAS {
		return nil, storage.ErrCASNotImplemented
	}
	entry, ok := s.segmentRef(loc.Ref)
	if !ok {
		return nil, fmt.Errorf("segment %s not found in metadata for %s/%s", ref.BlobID, s.bucket, s.key)
	}
	if entry.Size < 0 {
		return nil, fmt.Errorf("segment %s has invalid size %d", entry.BlobID, entry.Size)
	}
	if entry.Size == 0 {
		// Empty (0-byte) segment of an empty object: there is no shard to read.
		return &segmentBytesReadCloser{Reader: bytes.NewReader(nil), data: nil}, nil
	}

	record, err := s.placementRecord(entry)
	if err != nil {
		return nil, err
	}

	shardKey := s.key + "/segments/" + entry.BlobID
	rc, err := s.b.newECObjectReader().OpenObject(ctx, s.bucket, shardKey, record, entry.Size)
	if err != nil {
		return nil, fmt.Errorf("open segment %s: %w", entry.BlobID, err)
	}
	return rc, nil
}

type segmentBytesReadCloser struct {
	*bytes.Reader
	data []byte
}

func (r *segmentBytesReadCloser) Close() error { return nil }

func (r *segmentBytesReadCloser) SegmentBytes() []byte { return r.data }

func (s *clusterSegmentStore) ReadAtSegment(ctx context.Context, ref storage.SegmentRef, offset int64, buf []byte) (int, error) {
	if offset < 0 {
		return 0, fmt.Errorf("segment %s: negative offset %d", ref.BlobID, offset)
	}
	loc := storage.ParseLocator(ref.BlobID)
	if loc.Scheme == storage.LocatorCAS {
		return 0, storage.ErrCASNotImplemented
	}
	entry, ok := s.segmentRef(loc.Ref)
	if !ok {
		return 0, fmt.Errorf("segment %s not found in metadata for %s/%s", ref.BlobID, s.bucket, s.key)
	}
	if entry.Size <= 0 {
		return 0, fmt.Errorf("segment %s has invalid size %d", entry.BlobID, entry.Size)
	}
	if offset >= entry.Size {
		return 0, io.EOF
	}
	if max := entry.Size - offset; int64(len(buf)) > max {
		buf = buf[:max]
	}
	record, err := s.placementRecord(entry)
	if err != nil {
		return 0, err
	}
	shardKey := s.key + "/segments/" + entry.BlobID
	return s.b.newECObjectReader().ReadAt(ctx, s.bucket, shardKey, record, entry.Size, offset, buf)
}

type chunkedSegmentRangeStore interface {
	ReadAtSegment(ctx context.Context, ref storage.SegmentRef, offset int64, buf []byte) (int, error)
}

func readAtChunkedSegments(ctx context.Context, store chunkedSegmentRangeStore, refs []storage.SegmentRef, offset int64, buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	window, startOff, err := chunkedSegmentWindow(refs, offset, len(buf))
	if err != nil {
		return 0, err
	}
	done := 0
	segOff := startOff
	for _, ref := range window {
		if done == len(buf) {
			break
		}
		available := ref.Size - segOff
		if available <= 0 {
			segOff = 0
			continue
		}
		want := len(buf) - done
		if int64(want) > available {
			want = int(available)
		}
		n, readErr := store.ReadAtSegment(ctx, ref, segOff, buf[done:done+want])
		done += n
		if readErr != nil {
			if errors.Is(readErr, io.EOF) && done > 0 {
				return done, nil
			}
			return done, readErr
		}
		if n != want {
			return done, io.ErrUnexpectedEOF
		}
		segOff = 0
	}
	if done != len(buf) {
		return done, io.EOF
	}
	return done, nil
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
			Nodes:       cloneStringSlice(ref.NodeIDs),
			K:           int(ref.ECData),
			M:           int(ref.ECParity),
			StripeBytes: int(ref.StripeBytes),
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
			ECData:           entry.ECData,
			ECParity:         entry.ECParity,
			StripeBytes:      entry.StripeBytes,
			NodeIDs:          cloneStringSlice(entry.NodeIDs),
		}
	}
	return refs
}

// segmentRefsToMetaEntries is the inverse of segmentMetaEntriesToRefs: it
// projects storage.SegmentRef entries (the on-disk/owner-local segment list of
// an appendable object) into SegmentMetaEntry records for persistence in a
// PutObjectMetaCmd. SegmentIdx is assigned by ordinal so the entries keep a
// deterministic order. Used by the off-raft AppendObject RMW to rebuild the
// manifest blob's Segments slice after appending a new segment.
func segmentRefsToMetaEntries(refs []storage.SegmentRef) []SegmentMetaEntry {
	if len(refs) == 0 {
		return nil
	}
	entries := make([]SegmentMetaEntry, len(refs))
	for i, ref := range refs {
		entries[i] = SegmentMetaEntry{
			BlobID:           ref.BlobID,
			Size:             ref.Size,
			Checksum:         append([]byte(nil), ref.Checksum...),
			PlacementGroupID: ref.PlacementGroupID,
			ShardSize:        ref.ShardSize,
			SegmentIdx:       int32(i),
			ECData:           ref.ECData,
			ECParity:         ref.ECParity,
			StripeBytes:      ref.StripeBytes,
			NodeIDs:          cloneStringSlice(ref.NodeIDs),
		}
	}
	return entries
}
