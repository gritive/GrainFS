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

// WalkOrphanSegments walks <root>/data/<bucket>/<key>_segments/<blobID>
// returning files older than scrubOrphanAge and not in `known`.
// Bucket ENOENT (race with bucket delete) is treated as success/empty.
func (b *DistributedBackend) WalkOrphanSegments(bucket string, known map[string]bool, fn func(string) error) error {
	bucketDir := filepath.Join(b.root, "data", bucket)
	cutoff := time.Now().Add(-b.scrubOrphanAge)

	entries, err := os.ReadDir(bucketDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, segmentsDirSuffix) {
			continue
		}
		keyName := name[:len(name)-len(segmentsDirSuffix)]
		segDir := filepath.Join(bucketDir, name)
		segEntries, err := os.ReadDir(segDir)
		if err != nil {
			// Swallow per-segDir errors to keep the cycle making progress
			// across other dirs. Caller increments error metrics if needed.
			continue
		}
		for _, seg := range segEntries {
			if seg.IsDir() {
				continue
			}
			segPath := filepath.Join(segDir, seg.Name())
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
				return err
			}
		}
	}
	return nil
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
