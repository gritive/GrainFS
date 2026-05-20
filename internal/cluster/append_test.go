package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

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

	const segETag = "deadbeefcafebabedeadbeefcafebabe"
	seg := storage.SegmentRef{
		BlobID:   "blob-1",
		Size:     4,
		Checksum: bytes.Repeat([]byte{0xde}, storage.ChecksumLen),
	}
	cmd := AppendObjectCmd{
		Bucket:         "test",
		Key:            "k",
		ExpectedOffset: 0,
		BlobID:         seg.BlobID,
		SegmentSize:    seg.Size,
		SegmentETag:    segETag,
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

func TestAppendObjectSerializesSameObjectBeforeSegmentWrite(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()

	if err := b.CreateBucket(ctx, "test"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := b.AppendObject(ctx, "test", "k", 0, bytes.NewReader([]byte("aaaa"))); err != nil {
		t.Fatalf("initial AppendObject: %v", err)
	}

	firstEntered := make(chan struct{})
	secondEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	var entered atomic.Int32
	b.testBeforeAppendSegmentWrite = func() {
		switch entered.Add(1) {
		case 1:
			close(firstEntered)
			<-releaseFirst
		case 2:
			close(secondEntered)
		}
	}

	errCh := make(chan error, 2)
	go func() {
		_, err := b.AppendObject(ctx, "test", "k", 4, bytes.NewReader([]byte("bbbb")))
		errCh <- err
	}()
	<-firstEntered

	go func() {
		_, err := b.AppendObject(ctx, "test", "k", 4, bytes.NewReader([]byte("cccc")))
		errCh <- err
	}()

	select {
	case <-secondEntered:
		close(releaseFirst)
		<-errCh
		<-errCh
		t.Fatal("second same-object append reached segment write before the first append committed")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseFirst)

	successes, mismatches := 0, 0
	for i := 0; i < 2; i++ {
		err := <-errCh
		switch {
		case err == nil:
			successes++
		case errors.Is(err, storage.ErrAppendOffsetMismatch):
			mismatches++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if successes != 1 || mismatches != 1 {
		t.Fatalf("got %d success + %d mismatch; want 1+1", successes, mismatches)
	}
	if got := entered.Load(); got != 1 {
		t.Fatalf("segment write entries=%d, want 1", got)
	}
}

func TestAppendObjectReusesVersionIDAcrossSegments(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()

	if err := b.CreateBucket(ctx, "test"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	first, err := b.AppendObject(ctx, "test", "k", 0, bytes.NewReader([]byte("aaaa")))
	if err != nil {
		t.Fatalf("first AppendObject: %v", err)
	}
	second, err := b.AppendObject(ctx, "test", "k", 4, bytes.NewReader([]byte("bbbb")))
	if err != nil {
		t.Fatalf("second AppendObject: %v", err)
	}
	if first.VersionID == "" {
		t.Fatal("first append returned empty VersionID")
	}
	if second.VersionID != first.VersionID {
		t.Fatalf("second append VersionID=%q, want existing %q", second.VersionID, first.VersionID)
	}
}

func TestAppendObjectFSMApplyUsesCommandModifiedTime(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()

	if err := b.CreateBucket(ctx, "test"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	cmd := AppendObjectCmd{
		Bucket:          "test",
		Key:             "k",
		ExpectedOffset:  0,
		BlobID:          "blob-1",
		SegmentSize:     4,
		SegmentETag:     "deadbeefcafebabedeadbeefcafebabe",
		ModifiedUnixSec: 1234,
	}
	data, err := encodeAppendObjectCmd(cmd)
	if err != nil {
		t.Fatalf("encode AppendObjectCmd: %v", err)
	}

	if err := b.fsm.applyAppendObjectFromCmd(data); err != nil {
		t.Fatalf("applyAppendObjectFromCmd: %v", err)
	}

	obj, err := b.HeadObject(ctx, "test", "k")
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if obj.LastModified != 1234 {
		t.Fatalf("LastModified=%d, want 1234", obj.LastModified)
	}
}

func TestAppendObjectConvertsPlainPutAtCurrentOffset(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()

	if err := b.CreateBucket(ctx, "test"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := b.PutObject(ctx, "test", "k", bytes.NewReader([]byte("hello")), "text/plain"); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	obj, err := b.AppendObject(ctx, "test", "k", 5, bytes.NewReader([]byte("world")))
	if err != nil {
		t.Fatalf("AppendObject: %v", err)
	}
	if !obj.IsAppendable {
		t.Fatal("IsAppendable=false")
	}
	if obj.Size != 10 {
		t.Fatalf("size=%d, want 10", obj.Size)
	}

	rc, _, err := b.GetObject(ctx, "test", "k")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "helloworld" {
		t.Fatalf("body=%q, want helloworld", string(got))
	}
}

// TestAppendObjectFollowerHeadConsistent — Red 14. After leader-side
// AppendObject commits via data-Raft, follower HeadObject must observe the
// same Size and Segments. Multi-node harness is required to exercise this
// (current cluster test infra is single-node — newTestDistributedBackend);
// the behavior is verified end-to-end in the Phase 6 cluster e2e (Task 25).
func TestAppendObjectFollowerHeadConsistent(t *testing.T) {
	t.Skip("Red 14: requires multi-node cluster harness — deferred to Phase 6 e2e (Task 25)")
}

// TestAppendObjectForwardedFromNonOwner — Red 15. When the request enters a
// non-owner node, ClusterCoordinator.AppendObject must stream the body to the
// owner via ForwardSender.SendStream(ForwardOpAppendObject); the receiver
// (handleAppendObjectStream) executes the local AppendObject + commits
// ObjectIndex. Verified by HeadObject on the owner observing the new size +
// segments. Requires multi-node cluster harness — deferred to Phase 6 e2e
// (Task 25).
func TestAppendObjectForwardedFromNonOwner(t *testing.T) {
	t.Skip("Red 15: requires multi-node cluster harness — deferred to Phase 6 e2e (Task 25)")
}

// TestAppendObjectSyncsObjectIndex — Red 16. After AppendObject the meta-Raft
// ObjectIndex entry must reflect the new Size and ETag. Single-node coverage
// would require wiring an indexWriter onto newTestDistributedBackend (which
// has no ClusterCoordinator); the behavior is naturally covered by both the
// local-exec path (commitObjectIndex in ClusterCoordinator.AppendObject) and
// the forward path (ProposeObjectIndex in handleAppendObjectStream). End-to-
// end verification lands in Phase 6 cluster e2e (Task 25).
func TestAppendObjectSyncsObjectIndex(t *testing.T) {
	t.Skip("Red 16: ObjectIndex commit verified via Phase 6 cluster e2e (Task 25)")
}

// TestAppendObjectStalePlacementRetried — Red 17. Verifies the bounded
// transparent retry on ErrStalePlacement inside
// ClusterCoordinator.appendObjectLocalWithRetry. Requires a cluster harness
// that can flip the placement assignment between the coordinator's route
// resolve and the FSM apply — not feasible with the current single-node
// test infra. Behavior is design-asserted by code review and exercised
// indirectly via Phase 6 rebalance scenarios.
func TestAppendObjectStalePlacementRetried(t *testing.T) {
	t.Skip("Red 17: requires placement-rebalance fault injection — deferred to integration suite")
}
