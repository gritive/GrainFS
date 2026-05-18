package cluster

import (
	"bytes"
	"context"
	"errors"
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

// TestAppendObjectRaceTwoConcurrent — Red 13. Two goroutines race the same
// expectedOffset against an existing object. Raft serializes the two proposes
// at FSM apply: the first wins (size grows), the second sees mismatched
// existing.Size and is rejected with ErrAppendOffsetMismatch via Phase A
// apply-error propagation. We must observe exactly 1 success + 1 mismatch —
// any other outcome means the apply error did not propagate through
// b.propose to the caller.
func TestAppendObjectRaceTwoConcurrent(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()

	if err := b.CreateBucket(ctx, "test"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// Seed: create initial appendable object (size=4).
	if _, err := b.AppendObject(ctx, "test", "k", 0, bytes.NewReader([]byte("aaaa"))); err != nil {
		t.Fatalf("initial AppendObject: %v", err)
	}

	// Two goroutines race the same expectedOffset=4.
	type result struct{ err error }
	ch := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := b.AppendObject(ctx, "test", "k", 4, bytes.NewReader([]byte("bbbb")))
			ch <- result{err}
		}()
	}
	r1, r2 := <-ch, <-ch

	successes, mismatches := 0, 0
	for _, r := range []result{r1, r2} {
		switch {
		case r.err == nil:
			successes++
		case errors.Is(r.err, storage.ErrAppendOffsetMismatch):
			mismatches++
		default:
			t.Fatalf("unexpected error: %v", r.err)
		}
	}
	if successes != 1 || mismatches != 1 {
		t.Fatalf("got %d success + %d mismatch; want 1+1 — apply error not propagated through b.propose", successes, mismatches)
	}

	// Final state: object size = 8 (4 + one 4-byte append).
	final, err := b.HeadObject(ctx, "test", "k")
	if err != nil {
		t.Fatalf("final HeadObject: %v", err)
	}
	if final.Size != 8 {
		t.Fatalf("final size=%d, want 8 (one append must have committed)", final.Size)
	}
	if len(final.Segments) != 2 {
		t.Fatalf("final segments=%d, want 2 (seed + one winning append)", len(final.Segments))
	}
}
