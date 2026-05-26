package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestClusterOpenSegmentRejectsCAS(t *testing.T) {
	s := &clusterSegmentStore{bucket: "bkt", key: "obj"}
	_, err := s.OpenSegment(context.Background(), storage.SegmentRef{BlobID: "cas://b3-deadbeef", Size: 16})
	if !errors.Is(err, storage.ErrCASNotImplemented) {
		t.Fatalf("OpenSegment err = %v, want ErrCASNotImplemented", err)
	}
	_, err = s.ReadAtSegment(context.Background(), storage.SegmentRef{BlobID: "cas://b3-deadbeef", Size: 16}, 0, make([]byte, 4))
	if !errors.Is(err, storage.ErrCASNotImplemented) {
		t.Fatalf("ReadAtSegment err = %v, want ErrCASNotImplemented", err)
	}
}
