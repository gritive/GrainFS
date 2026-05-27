package cluster

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestSegmentKnownPath_MatchesWalker is the safety net for the orphan-segment
// GC: it ties the two independent production paths together. The known-set
// builder uses storage.SegmentKnownPath(bucket, key, blobID); the orphan walker
// (WalkOrphanSegments) derives the path it yields from the on-disk directory
// layout. If these ever drift for any (non-canonical) key, a referenced
// segment's known-set entry won't match the walker's yielded path, the segment
// is treated as orphan, and physical deletion causes permanent data loss.
func TestSegmentKnownPath_MatchesWalker(t *testing.T) {
	b := newTestDistributedBackend(t)
	bucket := "bucket"
	blobID := "01HXYZblob" // a bare/legacy blob id; ParseLocator(blobID).Ref == blobID

	// Age gate: WalkOrphanSegments skips files newer than scrubOrphanAge (5m
	// default). Backdate the segment mtime so it is always eligible.
	old := time.Now().Add(-10 * time.Minute)

	for _, key := range []string{"a/b", "a/b/", "a//b", "a/./b", "dir/sub/obj", "obj"} {
		// Write a segment blob on disk at the backend's real segment path.
		segPath := b.SegmentBlobPath(bucket, key, blobID)
		if err := os.MkdirAll(filepath.Dir(segPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(segPath, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(segPath, old, old); err != nil {
			t.Fatal(err)
		}

		// Walk with an EMPTY known-set so every eligible segment is yielded.
		var yielded []string
		err := b.WalkOrphanSegments(bucket, map[string]bool{}, func(p string) error {
			yielded = append(yielded, p)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		want := storage.SegmentKnownPath(bucket, key, blobID)
		found := false
		for _, y := range yielded {
			if y == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("key %q: SegmentKnownPath=%q not among walker-yielded paths %v -- mismatch => data loss", key, want, yielded)
		}

		// Cleanup for next iteration (a/b and a/b/ share the same on-disk dir).
		_ = os.RemoveAll(filepath.Dir(segPath))
	}
}
