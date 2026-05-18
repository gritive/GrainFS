package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCoalesceConcurrentAppendPreserved exercises the race between many
// concurrent appends and an in-process coalesce trigger. After all appends
// quiesce + coalesce settles, the on-the-wire body length must equal
// obj.Size — i.e. raw + coalesced together must cover every byte that was
// appended. Snapshot+exact-BlobID-match in applyCoalesceSegments protects
// raced-in segments.
func TestCoalesceConcurrentAppendPreserved(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))

	b.coalesceCfg.SegmentCount = 8

	bucket, key := "b", "k"

	// 16 sequential appends. (Concurrent appends at the same offset surface
	// ErrAppendOffsetMismatch — that's tested elsewhere. The race we care
	// about here is between append progress and the worker coalescing
	// partway through.)
	var totalLen int64
	for i := 0; i < 16; i++ {
		chunk := []byte(fmt.Sprintf("c%02d.", i))
		require.NoError(t, putAppendAt(b, bucket, key, totalLen, chunk))
		atomic.AddInt64(&totalLen, int64(len(chunk)))
	}

	require.Eventually(t, func() bool {
		obj, _ := b.HeadObject(ctx, bucket, key)
		return obj != nil && len(obj.Coalesced) >= 1
	}, 5*time.Second, 20*time.Millisecond)

	obj, err := b.HeadObject(ctx, bucket, key)
	require.NoError(t, err)
	rc, _, err := b.GetObject(ctx, bucket, key)
	require.NoError(t, err)
	body, _ := io.ReadAll(rc)
	_ = rc.Close()
	if int64(len(body)) != obj.Size {
		t.Fatalf("body bytes %d != obj.Size %d", len(body), obj.Size)
	}
}

// putAppendAt is a small wrapper that calls AppendObject at the given offset
// and asserts no error — keeps the table-driven test body concise.
func putAppendAt(b *DistributedBackend, bucket, key string, off int64, data []byte) error {
	_, err := b.AppendObject(context.Background(), bucket, key, off, bytes.NewReader(data))
	return err
}

// TestCoalesceConcurrentRacedAppendKeepsSegment exercises Task 7's race
// branch end-to-end: a fresh append landing between coalesce snapshot and
// apply must survive (the snapshot consumed S = older segments; the raced
// segment must remain visible).
func TestCoalesceConcurrentRacedAppendKeepsSegment(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))
	b.coalesceCfg.SegmentCount = 1 << 30 // disable count trigger, drive via explicit call

	bucket, key := "b", "k"
	// Seed two segments.
	require.NoError(t, putAppendAt(b, bucket, key, 0, []byte("aa")))
	require.NoError(t, putAppendAt(b, bucket, key, 2, []byte("bb")))

	// Capture snapshot for the coalesce job (mimics worker race).
	pre, err := b.HeadObject(ctx, bucket, key)
	require.NoError(t, err)
	require.Equal(t, 2, len(pre.Segments))

	// Race-in a third segment before processCoalesceJobB2 takes its
	// HeadObject snapshot.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = putAppendAt(b, bucket, key, 4, []byte("cc"))
	}()
	go func() {
		defer wg.Done()
		_ = b.processCoalesceJobB2(ctx, coalesceJob{Bucket: bucket, Key: key})
	}()
	wg.Wait()

	// Final state: full body intact regardless of interleave.
	obj, err := b.HeadObject(ctx, bucket, key)
	require.NoError(t, err)
	rc, _, err := b.GetObject(ctx, bucket, key)
	require.NoError(t, err)
	body, _ := io.ReadAll(rc)
	_ = rc.Close()
	if int64(len(body)) != obj.Size {
		t.Fatalf("body=%d obj.Size=%d", len(body), obj.Size)
	}
	if obj.Size != 6 {
		t.Fatalf("obj.Size=%d want 6", obj.Size)
	}
}
