package storage

import (
	"bytes"
	"context"
	"testing"
)

// TestCachedBackendAppendInvalidatesCache verifies that CachedBackend.AppendObject
// invalidates the (bucket, key) cache entry so a subsequent HeadObject reflects
// the new appended size instead of the stale cached one.
func TestCachedBackendAppendInvalidatesCache(t *testing.T) {
	inner := newTestLocalBackend(t)
	cb := NewCachedBackend(inner)
	ctx := context.Background()

	// Initial append creates the appendable object.
	if _, err := cb.AppendObject(ctx, "test", "k", 0, bytes.NewReader([]byte("hello"))); err != nil {
		t.Fatalf("initial append: %v", err)
	}

	// HEAD populates the cache with size=5.
	if _, err := cb.HeadObject(ctx, "test", "k"); err != nil {
		t.Fatalf("head: %v", err)
	}

	// Append more → cache must be invalidated.
	if _, err := cb.AppendObject(ctx, "test", "k", 5, bytes.NewReader([]byte("world"))); err != nil {
		t.Fatalf("second append: %v", err)
	}

	// HEAD after invalidation must reflect new size 10.
	obj, err := cb.HeadObject(ctx, "test", "k")
	if err != nil {
		t.Fatalf("head 2: %v", err)
	}
	if obj.Size != 10 {
		t.Fatalf("size=%d, want 10 (cache stale)", obj.Size)
	}
}
