package snapshot

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gritive/GrainFS/internal/storage"
)

// AllFrozenObjectVersions returns every (bucket, key, versionID) pinned by a
// live snapshot descriptor as a storage.SnapshotObjectRef. It is the
// full-object analogue of AllFrozenSegmentPaths: the EC orphan-shard scrubber
// maps each ref to a canonical shard dir and treats it as known, so a
// snapshot-pinned full-object EC version is never swept even after its live
// metadata is hard-deleted.
//
// Reads descriptors strictly: any unreadable/corrupt descriptor returns an
// error so the caller fails closed (skips the sweep) rather than sweeping
// against an incomplete known-set. Does NOT use Manager.List(), which silently
// skips corrupt files. Delete markers are excluded (no shards). Refs without a
// VersionID are excluded (no per-version shard dir to protect). Over-inclusion
// of chunked/coalesced objects is harmless: their shard dirs are skipped by the
// walker's path-class filter, and a full-object dir that does not exist is
// never yielded.
func (m *Manager) AllFrozenObjectVersions() ([]storage.SnapshotObjectRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return nil, fmt.Errorf("read snapshot dir: %w", err)
	}
	seen := make(map[string]struct{})
	var out []storage.SnapshotObjectRef
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "snapshot-") || !strings.HasSuffix(e.Name(), ".json.zst") {
			continue
		}
		snap, err := m.readSnapshot(filepath.Join(m.dir, e.Name()))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue // descriptor rotated away mid-scan; not corruption
			}
			return nil, fmt.Errorf("corrupt snapshot descriptor %s: %w", e.Name(), err)
		}
		for i := range snap.Objects {
			o := &snap.Objects[i]
			if o.IsDeleteMarker || o.VersionID == "" {
				continue
			}
			dedupKey := o.Bucket + "\x00" + o.Key + "\x00" + o.VersionID
			if _, dup := seen[dedupKey]; dup {
				continue
			}
			seen[dedupKey] = struct{}{}
			out = append(out, storage.SnapshotObjectRef{
				Bucket:    o.Bucket,
				Key:       o.Key,
				VersionID: o.VersionID,
			})
		}
	}
	return out, nil
}
