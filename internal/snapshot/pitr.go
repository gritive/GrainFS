package snapshot

import (
	"errors"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/wal"
)

// ErrNoSnapshotBefore is returned when no snapshot exists before the target time.
var ErrNoSnapshotBefore = errors.New("no snapshot found before target time")

// PITRResult holds the outcome of a PITR restore operation.
type PITRResult struct {
	RestoredObjects    int                 `json:"restored_objects"`
	WALEntriesReplayed int                 `json:"wal_entries_replayed"`
	StaleBlobs         []storage.StaleBlob `json:"stale_blobs"`
	BaseSnapshotSeq    uint64              `json:"base_snapshot_seq"`
}

// PITRRestore restores metadata to the state it was at targetTime.
// It finds the nearest snapshot at or before targetTime, restores it,
// then replays WAL entries up to targetTime.
func (m *Manager) PITRRestore(targetTime time.Time) (*PITRResult, error) {
	snaps, err := m.List()
	if err != nil {
		return nil, fmt.Errorf("list snapshots: %w", err)
	}

	// Find the latest snapshot with timestamp <= targetTime
	var base *Snapshot
	for _, s := range snaps {
		if !s.Timestamp.After(targetTime) {
			base = s
		}
	}
	if base == nil {
		return nil, ErrNoSnapshotBefore
	}

	// Build version map from base snapshot.
	objects := make(map[string]storage.SnapshotObject, len(base.Objects))
	for _, o := range base.Objects {
		objects[snapshotObjectKey(o.Bucket, o.Key, o.VersionID)] = o
	}

	// Replay WAL entries if WAL directory is configured
	walReplayed := 0
	if m.walDir != "" {
		walReplayed, err = wal.Replay(m.walDir, base.WALOffset, targetTime, func(e wal.Entry) {
			switch e.Op {
			case wal.OpPut:
				objects[snapshotObjectKey(e.Bucket, e.Key, e.VersionID)] = storage.SnapshotObject{
					Bucket:      e.Bucket,
					Key:         e.Key,
					ETag:        e.ETag,
					ContentType: e.ContentType,
					Size:        e.Size,
					Modified:    e.Timestamp / 1e9, // ns → s
					VersionID:   e.VersionID,
					IsLatest:    true,
				}
				clearLatest(objects, e.Bucket, e.Key, e.VersionID)
			case wal.OpDelete:
				if e.VersionID == "" {
					deleteAllVersions(objects, e.Bucket, e.Key)
					return
				}
				objects[snapshotObjectKey(e.Bucket, e.Key, e.VersionID)] = storage.SnapshotObject{
					Bucket:         e.Bucket,
					Key:            e.Key,
					ETag:           "DEL",
					VersionID:      e.VersionID,
					IsDeleteMarker: true,
					IsLatest:       true,
				}
				clearLatest(objects, e.Bucket, e.Key, e.VersionID)
			case wal.OpDeleteVersion:
				if e.VersionID != "" {
					delete(objects, snapshotObjectKey(e.Bucket, e.Key, e.VersionID))
				} else {
					deleteAllVersions(objects, e.Bucket, e.Key)
				}
			}
		})
		if err != nil {
			return nil, fmt.Errorf("wal replay: %w", err)
		}
	}

	normalizeLatest(objects)

	// Flatten map to slice
	finalObjects := make([]storage.SnapshotObject, 0, len(objects))
	for _, o := range objects {
		finalObjects = append(finalObjects, o)
	}

	restored, stale, err := m.backend.RestoreObjects(finalObjects)
	if err != nil {
		return nil, fmt.Errorf("restore objects: %w", err)
	}

	return &PITRResult{
		RestoredObjects:    restored,
		WALEntriesReplayed: walReplayed,
		StaleBlobs:         stale,
		BaseSnapshotSeq:    base.Seq,
	}, nil
}

func snapshotObjectKey(bucket, key, versionID string) string {
	return bucket + "\x00" + key + "\x00" + versionID
}

func clearLatest(objects map[string]storage.SnapshotObject, bucket, key, latestVersionID string) {
	for k, obj := range objects {
		if obj.Bucket != bucket || obj.Key != key || obj.VersionID == latestVersionID {
			continue
		}
		obj.IsLatest = false
		objects[k] = obj
	}
}

func deleteAllVersions(objects map[string]storage.SnapshotObject, bucket, key string) {
	for k, obj := range objects {
		if obj.Bucket == bucket && obj.Key == key {
			delete(objects, k)
		}
	}
}

func normalizeLatest(objects map[string]storage.SnapshotObject) {
	type groupKey struct{ bucket, key string }
	latestSeen := map[groupKey]bool{}
	newestKey := map[groupKey]string{}
	newestVersion := map[groupKey]string{}
	for mapKey, obj := range objects {
		g := groupKey{bucket: obj.Bucket, key: obj.Key}
		if obj.IsLatest {
			latestSeen[g] = true
		}
		if obj.VersionID >= newestVersion[g] {
			newestVersion[g] = obj.VersionID
			newestKey[g] = mapKey
		}
	}
	for g, mapKey := range newestKey {
		if latestSeen[g] {
			continue
		}
		obj := objects[mapKey]
		obj.IsLatest = true
		objects[mapKey] = obj
	}
}
