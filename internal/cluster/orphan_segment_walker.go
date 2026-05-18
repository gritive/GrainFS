package cluster

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gritive/GrainFS/internal/scrubber"
)

const segmentsDirSuffix = "_segments"

// WalkOrphanSegments recursively walks <root>/data/<bucket>/<key>_segments/<blobID>
// (key may contain `/`) returning files older than scrubOrphanAge and not in
// `known`. Bucket ENOENT (race with bucket delete) is treated as success/empty.
func (b *DistributedBackend) WalkOrphanSegments(bucket string, known map[string]bool, fn func(string) error) error {
	bucketDir := filepath.Join(b.root, "data", bucket)
	cutoff := time.Now().Add(-b.scrubOrphanAge)

	if _, err := os.Stat(bucketDir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil // bucket race with delete
		}
		return err
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
		return walkErr
	}
	return stopErr
}

// DeleteOrphanSegment removes one raw segment file. ENOENT is swallowed
// (idempotent — already removed by another path).
func (b *DistributedBackend) DeleteOrphanSegment(key string) error {
	path := filepath.Join(b.root, "data", key)
	err := os.Remove(path)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// Compile-time assertion: DistributedBackend satisfies OrphanSegmentWalkable.
var _ scrubber.OrphanSegmentWalkable = (*DistributedBackend)(nil)
