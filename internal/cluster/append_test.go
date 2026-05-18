package cluster

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestAppendObjectFSMApplyIdempotent — Red 12. Re-applying the same
// CmdAppendObject (same BlobID) must be a no-op so that Raft replay /
// duplicated apply doesn't double-count a segment. After two applies of the
// identical command the object must still have exactly 1 segment and the
// size of one segment.
func TestAppendObjectFSMApplyIdempotent(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()

	if err := b.CreateBucket(ctx, "test"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	seg := storage.SegmentRef{
		BlobID: "blob-1",
		Size:   4,
		ETag:   "deadbeefcafebabedeadbeefcafebabe",
	}
	cmd := AppendObjectCmd{
		Bucket:         "test",
		Key:            "k",
		ExpectedOffset: 0,
		BlobID:         seg.BlobID,
		SegmentSize:    seg.Size,
		SegmentETag:    seg.ETag,
	}
	data, err := encodeAppendObjectCmd(cmd)
	if err != nil {
		t.Fatalf("encode AppendObjectCmd: %v", err)
	}

	// First apply — creates appendable objectMeta with 1 segment.
	if err := b.fsm.applyAppendObjectFromCmd(data); err != nil {
		t.Fatalf("first apply: %v", err)
	}

	obj1, err := b.HeadObject(ctx, "test", "k")
	if err != nil {
		t.Fatalf("HeadObject after first apply: %v", err)
	}
	if len(obj1.Segments) != 1 || obj1.Segments[0].BlobID != "blob-1" {
		t.Fatalf("after first apply: segments=%+v, want 1 segment with BlobID=blob-1", obj1.Segments)
	}
	if obj1.Size != seg.Size {
		t.Fatalf("after first apply: size=%d, want %d", obj1.Size, seg.Size)
	}
	if !obj1.IsAppendable {
		t.Fatal("after first apply: IsAppendable=false, want true")
	}

	// Re-apply same BlobID → must be idempotent no-op.
	if err := b.fsm.applyAppendObjectFromCmd(data); err != nil {
		t.Fatalf("re-apply: %v", err)
	}

	obj2, err := b.HeadObject(ctx, "test", "k")
	if err != nil {
		t.Fatalf("HeadObject after re-apply: %v", err)
	}
	if len(obj2.Segments) != 1 {
		t.Fatalf("re-apply broke idempotency: segments=%d, want 1", len(obj2.Segments))
	}
	if obj2.Size != seg.Size {
		t.Fatalf("re-apply changed size: %d, want %d", obj2.Size, seg.Size)
	}
}
