package raft

import (
	"log/slog"
	"sync"
)

// Snapshotter creates and restores state machine snapshots.
type Snapshotter interface {
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

// SnapshotConfig controls when automatic snapshots are taken.
type SnapshotConfig struct {
	// Threshold is the number of applied entries since the last snapshot
	// before a new snapshot is triggered.
	Threshold uint64
}

// SnapshotManager handles automatic snapshot creation, log compaction,
// and snapshot restoration on startup.
type SnapshotManager struct {
	mu            sync.Mutex
	store         LogStore
	snapshotter   Snapshotter
	config        SnapshotConfig
	lastSnapIndex uint64
}

// NewSnapshotManager creates a snapshot manager.
func NewSnapshotManager(store LogStore, snap Snapshotter, config SnapshotConfig) *SnapshotManager {
	return &SnapshotManager{
		store:       store,
		snapshotter: snap,
		config:      config,
	}
}

// MaybeTrigger checks if a snapshot should be taken based on the number of
// applied entries since the last snapshot. Returns true if a snapshot was taken.
func (m *SnapshotManager) MaybeTrigger(appliedIndex, appliedTerm uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.config.Threshold == 0 {
		return false
	}

	if appliedIndex <= m.lastSnapIndex {
		return false
	}

	entriesSinceSnap := appliedIndex - m.lastSnapIndex
	if entriesSinceSnap < m.config.Threshold {
		return false
	}

	// Take snapshot
	data, err := m.snapshotter.Snapshot()
	if err != nil {
		slog.Error("snapshot: create failed", "error", err)
		return false
	}

	// Save snapshot to store
	if err := m.store.SaveSnapshot(appliedIndex, appliedTerm, data); err != nil {
		slog.Error("snapshot: save failed", "error", err)
		return false
	}

	// Compact log: remove all entries (requires InstallSnapshot RPC for follower recovery)
	if err := m.store.TruncateAfter(0); err != nil {
		slog.Warn("snapshot: log compaction failed", "error", err)
		// Snapshot is saved; compaction failure is non-fatal
	}

	m.lastSnapIndex = appliedIndex
	return true
}

// Restore loads the latest snapshot from the store and applies it to the
// state machine. Returns the snapshot index (0 if no snapshot exists).
func (m *SnapshotManager) Restore() (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	index, _, data, err := m.store.LoadSnapshot()
	if err != nil {
		return 0, err
	}

	// No snapshot
	if data == nil {
		return 0, nil
	}

	if err := m.snapshotter.Restore(data); err != nil {
		return 0, err
	}

	m.lastSnapIndex = index
	return index, nil
}

