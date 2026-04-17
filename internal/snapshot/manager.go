package snapshot

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

// Manager manages snapshot creation, listing, restore, and deletion.
type Manager struct {
	dir     string
	backend storage.Snapshotable
	nextSeq atomic.Uint64
}

// NewManager creates a Manager backed by the given snapshotable backend.
// snapshotDir is the directory where snapshot files are stored.
func NewManager(snapshotDir string, backend storage.Snapshotable) (*Manager, error) {
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		return nil, fmt.Errorf("create snapshot dir: %w", err)
	}
	m := &Manager{dir: snapshotDir, backend: backend}
	// Seed nextSeq from existing snapshots
	snaps, err := m.List()
	if err != nil {
		return nil, err
	}
	var maxSeq uint64
	for _, s := range snaps {
		if s.Seq > maxSeq {
			maxSeq = s.Seq
		}
	}
	m.nextSeq.Store(maxSeq)
	return m, nil
}

// Create takes a snapshot of the current metadata state.
func (m *Manager) Create(reason string) (*Snapshot, error) {
	objects, err := m.backend.ListAllObjects()
	if err != nil {
		return nil, fmt.Errorf("list objects: %w", err)
	}

	// Collect unique buckets
	bucketSet := make(map[string]bool)
	var totalSize int64
	for _, o := range objects {
		bucketSet[o.Bucket] = true
		totalSize += o.Size
	}
	buckets := make([]string, 0, len(bucketSet))
	for b := range bucketSet {
		buckets = append(buckets, b)
	}
	sort.Strings(buckets)

	seq := m.nextSeq.Add(1)
	snap := &Snapshot{
		Seq:         seq,
		Timestamp:   time.Now().UTC(),
		WALOffset:   0,
		Reason:      reason,
		ObjectCount: len(objects),
		SizeBytes:   totalSize,
		Buckets:     buckets,
		Objects:     objects,
	}

	// Atomic write: write to .tmp then rename
	tmpPath := m.path(seq) + ".tmp"
	finalPath := m.path(seq)
	if err := writeSnapshot(tmpPath, snap); err != nil {
		os.Remove(tmpPath)
		return nil, fmt.Errorf("write snapshot: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		return nil, fmt.Errorf("commit snapshot: %w", err)
	}
	return snap, nil
}

// List returns all available snapshots sorted by seq ascending.
func (m *Manager) List() ([]*Snapshot, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return nil, fmt.Errorf("read snapshot dir: %w", err)
	}
	var snaps []*Snapshot
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "snapshot-") || !strings.HasSuffix(e.Name(), ".json.gz") {
			continue
		}
		snap, err := readSnapshot(filepath.Join(m.dir, e.Name()))
		if err != nil {
			continue // skip corrupt files
		}
		snaps = append(snaps, snap)
	}
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].Seq < snaps[j].Seq })
	return snaps, nil
}

// Restore restores metadata from the snapshot with the given seq.
// Returns the number of restored objects and any stale blobs.
func (m *Manager) Restore(seq uint64) (restoredCount int, staleBlobs []storage.StaleBlob, err error) {
	snap, err := readSnapshot(m.path(seq))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil, ErrNotFound
		}
		return 0, nil, fmt.Errorf("read snapshot %d: %w", seq, err)
	}
	return m.backend.RestoreObjects(snap.Objects)
}

// Delete removes the snapshot file for the given seq.
func (m *Manager) Delete(seq uint64) error {
	p := m.path(seq)
	if err := os.Remove(p); err != nil {
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return err
	}
	return nil
}

func (m *Manager) path(seq uint64) string {
	return filepath.Join(m.dir, "snapshot-"+strconv.FormatUint(seq, 10)+".json.gz")
}

// ErrNotFound indicates the snapshot does not exist.
var ErrNotFound = fmt.Errorf("snapshot not found")
