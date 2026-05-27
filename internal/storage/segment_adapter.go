package storage

import (
	"context"
	"io"
	"os"
)

// localBackendAdapter routes each SegmentWriter chunk through
// LocalBackend.WriteSegmentBlob.
type localBackendAdapter struct{ b *LocalBackend }

func (a localBackendAdapter) WriteSegment(ctx context.Context, bucket, key string, idx int, r io.Reader) (SegmentRef, error) {
	_ = ctx
	_ = idx
	return a.b.WriteSegmentBlob(bucket, key, r)
}

// localSegmentStore opens individual segment blobs for the parallel
// SegmentReader. Each call returns a fresh ReadCloser positioned at the
// start of the segment.
type localSegmentStore struct {
	b      *LocalBackend
	bucket string
	key    string
}

func (s localSegmentStore) OpenSegment(ctx context.Context, ref SegmentRef) (io.ReadCloser, error) {
	_ = ctx
	loc := ParseLocator(ref.BlobID)
	if loc.Scheme == LocatorCAS {
		return nil, ErrCASNotImplemented
	}
	// LocatorLegacy: existing key-scoped physical path (unchanged behavior).
	blobID := loc.Ref
	path := s.b.segmentPath(s.bucket, s.key, blobID)
	if s.b.segEnc != nil {
		return openEncryptedObjectFile(path, s.b.segEnc, segmentFileAADFields(s.bucket, s.key, blobID), ref.Size)
	}
	return os.Open(path)
}
