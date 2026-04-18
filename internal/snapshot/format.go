package snapshot

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

// Snapshot is the on-disk representation of a metadata snapshot.
type Snapshot struct {
	Seq         uint64                   `json:"seq"`
	Timestamp   time.Time                `json:"timestamp"`
	WALOffset   uint64                   `json:"wal_offset"`
	Reason      string                   `json:"reason,omitempty"`
	ObjectCount int                      `json:"object_count"`
	SizeBytes   int64                    `json:"size_bytes"`
	Buckets     []string                 `json:"buckets"`
	Objects     []storage.SnapshotObject `json:"objects"`
	// BucketMeta is populated when the backend implements storage.BucketSnapshotable.
	// Older snapshots omit this field; Restore treats nil as a no-op for bucket state.
	BucketMeta []storage.SnapshotBucket `json:"bucket_meta,omitempty"`
}

func writeSnapshot(path string, snap *Snapshot) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	gz := gzip.NewWriter(f)
	enc := json.NewEncoder(gz)
	enc.SetIndent("", "")
	if err := enc.Encode(snap); err != nil {
		gz.Close()
		f.Close()
		return err
	}
	if err := gz.Close(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func readSnapshot(path string) (*Snapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	data, err := io.ReadAll(gz)
	if err != nil {
		return nil, err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}
