package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gritive/GrainFS/internal/scrubber"
)

const segmentsDirSuffix = "_segments"

// SetOwningGroupBackendSource wires the resolver that maps a bucket to its owning
// data-group's backend so the orphan-SEGMENT sweep can dispatch each per-bucket op
// to the group whose b.root subtree holds that bucket's segments. Call once during
// boot, before the scrubber starts. nil/un-wired => every bucket resolves to this
// backend (single-group).
func (b *DistributedBackend) SetOwningGroupBackendSource(fn func(bucket string) *DistributedBackend) {
	b.owningGroupBackendFn = fn
}

// owningGroupBackend resolves the backend that owns `bucket`'s segments. Defaults
// to this backend when un-wired (single-group). nil means the owner is not locally
// hosted (its segments are not on this node).
func (b *DistributedBackend) owningGroupBackend(bucket string) *DistributedBackend {
	if b.owningGroupBackendFn == nil {
		return b
	}
	return b.owningGroupBackendFn(bucket)
}

// SegmentSweepBuckets returns the de-duplicated union of every locally-hosted
// group's buckets, so the scrubber's per-bucket segment sweep covers all groups
// this node hosts (not just group-0). Fail-closed: any group's ListBuckets error
// returns an error so the scrubber skips the segment sweep this cycle. Default
// (un-wired hostedGroupBackendsSrc => []{b}) == this backend's ListBuckets (self).
func (b *DistributedBackend) SegmentSweepBuckets(ctx context.Context) ([]string, error) {
	seen := make(map[string]bool)
	var out []string
	for _, gb := range b.hostedGroupBackends() {
		if gb == nil {
			return nil, fmt.Errorf("segment sweep buckets: hosted group backend is nil")
		}
		bs, err := gb.ListBuckets(ctx)
		if err != nil {
			return nil, fmt.Errorf("segment sweep buckets: list group buckets: %w", err)
		}
		for _, bk := range bs {
			if !seen[bk] {
				seen[bk] = true
				out = append(out, bk)
			}
		}
	}
	return out, nil
}

// WalkOrphanSegments dispatches the per-bucket segment walk to the backend that
// owns `bucket` (segments live under that group's b.root), and skips the bucket
// when its owning group is not locally hosted or not caught-up (only the caught-up
// leader of a group GCs its segments — a lagging/follower FSM could mark a
// committed segment orphan). Implements scrubber.OrphanSegmentWalkable.
func (b *DistributedBackend) WalkOrphanSegments(bucket string, known map[string]bool, fn func(string) error) error {
	gb := b.owningGroupBackend(bucket)
	if gb == nil || !gb.CaughtUp(context.Background()) {
		return nil
	}
	return gb.walkOwnOrphanSegments(bucket, known, fn)
}

// walkOwnOrphanSegments recursively walks <root>/data/<bucket>/<key>_segments/<blobID>
// (key may contain `/`) returning files older than scrubOrphanAge and not in
// `known`. Bucket ENOENT (race with bucket delete) is treated as success/empty.
func (b *DistributedBackend) walkOwnOrphanSegments(bucket string, known map[string]bool, fn func(string) error) error {
	bucketDir := filepath.Join(b.root, "data", bucket)
	cutoff := time.Now().Add(-b.scrubOrphanAge)

	if _, err := os.Stat(bucketDir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil // bucket race with delete
		}
		return fmt.Errorf("stat bucket dir %s: %w", bucket, err)
	}

	var stopErr error
	walkErr := filepath.WalkDir(bucketDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			// Permission denied or other readdir error: skip this subtree, keep going.
			if d != nil && d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, segmentsDirSuffix) {
			return nil
		}
		// Reconstruct the S3 key prefix from path relative to bucketDir.
		// path = bucketDir + "/" + <key>_segments
		rel, relErr := filepath.Rel(bucketDir, path)
		if relErr != nil {
			return nil
		}
		// rel = "<key>_segments" (key may contain slashes)
		keyName := strings.TrimSuffix(rel, segmentsDirSuffix)
		// Convert backslashes to slashes for cross-platform consistency.
		keyName = filepath.ToSlash(keyName)
		segEntries, dirErr := os.ReadDir(path)
		if dirErr != nil {
			return filepath.SkipDir
		}
		for _, seg := range segEntries {
			if seg.IsDir() {
				continue
			}
			segPath := filepath.Join(path, seg.Name())
			info, statErr := os.Stat(segPath)
			if statErr != nil {
				continue
			}
			if info.ModTime().After(cutoff) {
				continue
			}
			key := bucket + "/" + keyName + segmentsDirSuffix + "/" + seg.Name()
			if known[key] {
				continue
			}
			if err := fn(key); err != nil {
				stopErr = err
				return filepath.SkipAll
			}
		}
		// Don't descend into a *_segments directory.
		return filepath.SkipDir
	})
	if walkErr != nil {
		return fmt.Errorf("walk bucket %s: %w", bucket, walkErr)
	}
	return stopErr
}

// DeleteOrphanSegment dispatches the delete to the backend that owns the segment's
// bucket (the key is `<bucket>/<key>_segments/<blobID>`, bucket is its first path
// component — buckets forbid '/'). Owner not locally hosted => no-op. Implements
// scrubber.OrphanSegmentWalkable.
func (b *DistributedBackend) DeleteOrphanSegment(key string) error {
	bucket, _, _ := strings.Cut(key, "/")
	gb := b.owningGroupBackend(bucket)
	if gb == nil {
		return nil
	}
	return gb.deleteOwnOrphanSegment(key)
}

// deleteOwnOrphanSegment removes one raw segment file under this backend's root.
// ENOENT is swallowed (idempotent — already removed by another path).
func (b *DistributedBackend) deleteOwnOrphanSegment(key string) error {
	path := filepath.Join(b.root, "data", key)
	err := os.Remove(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("remove orphan segment %s: %w", key, err)
	}
	return nil
}

// Compile-time assertion: DistributedBackend satisfies OrphanSegmentWalkable.
var _ scrubber.OrphanSegmentWalkable = (*DistributedBackend)(nil)
