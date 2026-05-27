package snapshot

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gritive/GrainFS/internal/storage"
)

// AllFrozenSegmentPaths returns every raw-segment path pinned by a live snapshot
// descriptor, grouped by bucket, in the canonical storage.SegmentKnownPath form
// (byte-identical to the scrubber's known-set). Reads descriptors strictly: any
// unreadable/corrupt descriptor returns an error so the scrubber fails closed
// (skips its sweep) rather than sweeping against an incomplete known-set. Does
// NOT use Manager.List(), which silently skips corrupt files.
func (m *Manager) AllFrozenSegmentPaths() (map[string][]string, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return nil, fmt.Errorf("read snapshot dir: %w", err)
	}
	seen := make(map[string]struct{})
	out := make(map[string][]string)
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "snapshot-") || !strings.HasSuffix(e.Name(), ".json.zst") {
			continue
		}
		snap, err := readSnapshot(filepath.Join(m.dir, e.Name()))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue // descriptor rotated away mid-scan; not corruption
			}
			return nil, fmt.Errorf("corrupt snapshot descriptor %s: %w", e.Name(), err)
		}
		for i := range snap.Objects {
			o := &snap.Objects[i]
			for _, seg := range o.Segments {
				p := storage.SegmentKnownPath(o.Bucket, o.Key, seg.BlobID)
				if _, dup := seen[p]; dup {
					continue
				}
				seen[p] = struct{}{}
				out[o.Bucket] = append(out[o.Bucket], p)
			}
		}
	}
	return out, nil
}
