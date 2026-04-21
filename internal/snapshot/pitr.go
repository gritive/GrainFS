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

	// Build object map from base snapshot
	objects := make(map[string]storage.SnapshotObject, len(base.Objects))
	for _, o := range base.Objects {
		objects[o.Bucket+"/"+o.Key] = o
	}

	// Replay WAL entries if WAL directory is configured
	walReplayed := 0
	if m.walDir != "" {
		walReplayed, err = wal.Replay(m.walDir, base.WALOffset, targetTime, func(e wal.Entry) {
			switch e.Op {
			case wal.OpPut:
				objects[e.Bucket+"/"+e.Key] = storage.SnapshotObject{
					Bucket:      e.Bucket,
					Key:         e.Key,
					ETag:        e.ETag,
					ContentType: e.ContentType,
					Size:        e.Size,
					Modified:    e.Timestamp / 1e9, // ns → s
				}
			case wal.OpDelete:
				// Latest-pointer delete (tombstone). PITR resolves the object map to
				// a single latest state per key, so drop the entry.
				delete(objects, e.Bucket+"/"+e.Key)
			case wal.OpDeleteVersion:
				// Hard delete of a specific version. Only affects the resolved state
				// when that version is the one currently represented in the map.
				// Without per-version tracking we approximate by removing the key —
				// a later PUT within the replay window re-inserts it.
				delete(objects, e.Bucket+"/"+e.Key)
			}
		})
		if err != nil {
			return nil, fmt.Errorf("wal replay: %w", err)
		}
	}

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
