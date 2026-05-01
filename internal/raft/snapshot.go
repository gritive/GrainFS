package raft

import (
	"fmt"
	"sync"

	"github.com/rs/zerolog/log"
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

	// TrailingLogs is the number of log entries to retain on disk after
	// a snapshot is taken. 0 = remove all (original behavior).
	TrailingLogs uint64
}

// SnapshotResult reports a snapshot that was successfully created.
type SnapshotResult struct {
	Index     uint64
	Term      uint64
	SizeBytes int
}

// SnapshotStatus reports the latest snapshot persisted in the LogStore.
type SnapshotStatus struct {
	Available bool
	Index     uint64
	Term      uint64
	SizeBytes int
}

// JointStateProvider returns the §4.3 joint consensus state at snapshot trigger
// time. Phase is the int8 form of jointPhase (0=None, 1=Entering). Empty slices /
// zero index when JointPhase is None. managedLearners is the set of learner IDs
// added by ChangeMembership (PR-K3); nil when none.
type JointStateProvider func() (phase int8, jointOldVoters, jointNewVoters []string, jointEnterIndex uint64, managedLearners []string)

// JointStateRestorer is called by SnapshotManager.Restore with the joint state
// stored alongside the snapshot. The implementation should adopt those fields
// onto the Node (typically Node.RestoreJointStateFromSnapshot).
type JointStateRestorer func(phase int8, jointOldVoters, jointNewVoters []string, jointEnterIndex uint64, managedLearners []string)

// SnapshotManager handles automatic snapshot creation, log compaction,
// and snapshot restoration on startup.
type SnapshotManager struct {
	mu            sync.Mutex
	store         LogStore
	snapshotter   Snapshotter
	config        SnapshotConfig
	lastSnapIndex uint64

	// Optional joint state hooks. nil providers / restorers leave joint fields
	// at zero, which is correct for callers that never enter joint consensus.
	jointStateProvider JointStateProvider
	jointStateRestorer JointStateRestorer
}

// NewSnapshotManager creates a snapshot manager.
func NewSnapshotManager(store LogStore, snap Snapshotter, config SnapshotConfig) *SnapshotManager {
	return &SnapshotManager{
		store:       store,
		snapshotter: snap,
		config:      config,
	}
}

// SetJointStateProvider wires the §4.3 capture hook used during MaybeTrigger.
// Pass Node.JointSnapshotState (or equivalent). Safe to leave unset for callers
// that never enter joint consensus.
func (m *SnapshotManager) SetJointStateProvider(fn JointStateProvider) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jointStateProvider = fn
}

// SetJointStateRestorer wires the §4.3 apply hook used during Restore.
func (m *SnapshotManager) SetJointStateRestorer(fn JointStateRestorer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jointStateRestorer = fn
}

// MaybeTrigger checks if a snapshot should be taken based on the number of
// applied entries since the last snapshot. Returns true if a snapshot was taken.
func (m *SnapshotManager) MaybeTrigger(appliedIndex, appliedTerm uint64, servers []Server) bool {
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

	if _, err := m.createSnapshotLocked(appliedIndex, appliedTerm, servers); err != nil {
		log.Error().Err(err).Msg("snapshot: trigger failed")
		return false
	}
	return true
}

// Trigger forces a snapshot at the caller-provided applied index, regardless of
// the automatic threshold. It is intended for operator/admin flows.
func (m *SnapshotManager) Trigger(appliedIndex, appliedTerm uint64, servers []Server) (SnapshotResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.createSnapshotLocked(appliedIndex, appliedTerm, servers)
}

// Status reports the latest persisted snapshot without mutating state.
func (m *SnapshotManager) Status() (SnapshotStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	snap, err := m.store.LoadSnapshot()
	if err != nil {
		return SnapshotStatus{}, err
	}
	if snap.Index == 0 {
		return SnapshotStatus{}, nil
	}
	return SnapshotStatus{
		Available: true,
		Index:     snap.Index,
		Term:      snap.Term,
		SizeBytes: len(snap.Data),
	}, nil
}

func (m *SnapshotManager) createSnapshotLocked(appliedIndex, appliedTerm uint64, servers []Server) (SnapshotResult, error) {
	if appliedIndex == 0 {
		return SnapshotResult{}, fmt.Errorf("snapshot: applied index is zero")
	}

	data, err := m.snapshotter.Snapshot()
	if err != nil {
		return SnapshotResult{}, fmt.Errorf("snapshot: create: %w", err)
	}

	// §4.3 joint state at snapshot point. Provider returns zero values when
	// not in a joint cycle; legacy callers without a provider also get zeros.
	var jPhase int8
	var jOld, jNew []string
	var jIdx uint64
	var jManaged []string
	if m.jointStateProvider != nil {
		jPhase, jOld, jNew, jIdx, jManaged = m.jointStateProvider()
	}

	if err := m.store.SaveSnapshot(Snapshot{
		Index:                appliedIndex,
		Term:                 appliedTerm,
		Data:                 data,
		Servers:              servers,
		JointPhase:           jointPhase(jPhase),
		JointOldVoters:       jOld,
		JointNewVoters:       jNew,
		JointEnterIndex:      jIdx,
		JointManagedLearners: jManaged,
	}); err != nil {
		return SnapshotResult{}, fmt.Errorf("snapshot: save: %w", err)
	}

	if m.config.TrailingLogs == 0 {
		// Original behavior: remove all log entries.
		if err := m.store.TruncateAfter(0); err != nil {
			log.Warn().Err(err).Msg("snapshot: disk log compaction failed")
		}
	} else if appliedIndex+1 > m.config.TrailingLogs {
		keepFrom := appliedIndex + 1 - m.config.TrailingLogs
		if err := m.store.TruncateBefore(keepFrom); err != nil {
			log.Warn().Err(err).Msg("snapshot: disk log compaction failed")
		}
	}

	m.lastSnapIndex = appliedIndex
	return SnapshotResult{Index: appliedIndex, Term: appliedTerm, SizeBytes: len(data)}, nil
}

// Restore loads the latest snapshot from the store and applies it to the
// state machine. Returns the snapshot index (0 if no snapshot exists).
func (m *SnapshotManager) Restore() (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	snap, err := m.store.LoadSnapshot()
	if err != nil {
		return 0, err
	}

	// Index == 0 means no snapshot has been saved (Raft indices are 1-based).
	if snap.Index == 0 {
		return 0, nil
	}

	if err := m.snapshotter.Restore(snap.Data); err != nil {
		return 0, err
	}

	// §4.3 joint state restoration. Triggered after FSM restore so any leader
	// promotion that follows has the correct phase to drive checkJointAdvance.
	if m.jointStateRestorer != nil {
		m.jointStateRestorer(int8(snap.JointPhase), snap.JointOldVoters, snap.JointNewVoters, snap.JointEnterIndex, snap.JointManagedLearners)
	}

	m.lastSnapIndex = snap.Index
	return snap.Index, nil
}
