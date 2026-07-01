package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/zstdpool"
)

type clusterSegmentStore struct {
	b      *DistributedBackend
	bucket string
	key    string
	obj    *storage.Object

	// Single decompressed-segment cache, scoped to one ranged GET. A stateless
	// store (created fresh per ReadAt) leaves these zero and never hits; a
	// stateful store reused across a GET's 5 MiB refills caches the last touched
	// compressed segment's plaintext so it is decompressed once, not once per
	// refill. cachedSegKey is the blob ID; a nil cachedSegPlaintext is a miss.
	cachedSegKey       string
	cachedSegPlaintext []byte
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

	storedSize := entry.Size
	if entry.StoredSize > 0 {
		storedSize = entry.StoredSize
	}
	shardKey := s.key + "/segments/" + entry.BlobID
	rc, err := s.b.newECObjectReader().OpenObject(ctx, s.bucket, shardKey, record, storedSize)
	if err != nil {
		return nil, fmt.Errorf("open segment %s: %w", entry.BlobID, err)
	}
	exact := &exactSegmentReadCloser{rc: rc, segment: entry.BlobID, expected: storedSize, remaining: storedSize}
	if entry.StoredSize == 0 {
		return exact, nil
	}
	// Compressed: read the whole (≤16 MiB) compressed segment and decompress to
	// plaintext. Segment-bounded buffering; range reads use ReadAtSegment.
	compressed, err := io.ReadAll(exact)
	if cerr := exact.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		return nil, fmt.Errorf("read compressed segment %s: %w", entry.BlobID, err)
	}
	plain, err := zstdpool.Decompress(compressed)
	if err != nil {
		return nil, fmt.Errorf("decompress segment %s: %w", entry.BlobID, err)
	}
	if int64(len(plain)) != entry.Size {
		return nil, fmt.Errorf("decompress segment %s: size %d != expected %d", entry.BlobID, len(plain), entry.Size)
	}
	return &segmentBytesReadCloser{Reader: bytes.NewReader(plain), data: plain}, nil
}

type segmentBytesReadCloser struct {
	*bytes.Reader
	data []byte
}

func (r *segmentBytesReadCloser) Close() error { return nil }

func (r *segmentBytesReadCloser) SegmentBytes() []byte { return r.data }

type exactSegmentReadCloser struct {
	rc        io.ReadCloser
	segment   string
	expected  int64
	remaining int64
	probed    bool
}

func (r *exactSegmentReadCloser) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.remaining > 0 {
		if int64(len(p)) > r.remaining {
			p = p[:r.remaining]
		}
		n, err := r.rc.Read(p)
		r.remaining -= int64(n)
		if err == io.EOF && r.remaining > 0 {
			return n, fmt.Errorf("segment %s reconstructed size short by %d bytes: %w", r.segment, r.remaining, io.ErrUnexpectedEOF)
		}
		return n, err
	}
	if r.probed {
		return 0, io.EOF
	}
	r.probed = true
	var extra [1]byte
	n, err := r.rc.Read(extra[:])
	if n > 0 {
		return 0, fmt.Errorf("segment %s reconstructed size exceeds metadata size %d", r.segment, r.expected)
	}
	if err != nil && err != io.EOF {
		return 0, err
	}
	return 0, io.EOF
}

func (r *exactSegmentReadCloser) Close() error {
	return r.rc.Close()
}

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
	if entry.StoredSize > 0 {
		// Compressed: no random access into the zstd frame. Decompress the whole
		// (≤16 MiB) segment once, cache the plaintext for the GET's remaining
		// refills, then slice from offset. The buf was already clamped to
		// entry.Size-offset above, and plaintext length == entry.Size, so the
		// copy fills buf exactly.
		plain, err := s.decompressedSegment(ctx, ref, loc.Ref)
		if err != nil {
			return 0, err
		}
		if offset >= int64(len(plain)) {
			return 0, io.EOF
		}
		n := copy(buf, plain[offset:])
		return n, nil
	}
	record, err := s.placementRecord(entry)
	if err != nil {
		return 0, err
	}
	shardKey := s.key + "/segments/" + entry.BlobID
	return s.b.newECObjectReader().ReadAt(ctx, s.bucket, shardKey, record, entry.Size, offset, buf)
}

// decompressedSegment returns the plaintext of a compressed segment, serving it
// from the per-GET single-segment cache when segKey matches the last decompress.
// On a miss it reuses OpenSegment (which for a compressed segment returns a
// full-buffer SegmentBytes provider), so this never re-implements the
// ReadAll+Decompress logic. segKey is the parsed blob ref (loc.Ref).
func (s *clusterSegmentStore) decompressedSegment(ctx context.Context, ref storage.SegmentRef, segKey string) ([]byte, error) {
	if s.cachedSegPlaintext != nil && s.cachedSegKey == segKey {
		return s.cachedSegPlaintext, nil
	}
	rc, err := s.OpenSegment(ctx, ref)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	sb, ok := rc.(interface{ SegmentBytes() []byte })
	if !ok {
		return nil, fmt.Errorf("segment %s: compressed OpenSegment did not return a buffered plaintext provider", ref.BlobID)
	}
	plain := sb.SegmentBytes()
	s.cachedSegKey = segKey
	s.cachedSegPlaintext = plain
	return plain, nil
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
			StoredSize:       entry.StoredSize,
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
			StoredSize:       ref.StoredSize,
		}
	}
	return entries
}
